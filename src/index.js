const request = require('request')
const es = require('event-stream')
const csv = require('csv-stream')
const { Pool } = require('pg')
const copyFrom = require('pg-copy-streams').from
const dotenv = require('dotenv')

dotenv.config()

// connect via pool with db
const pool = new Pool({
  connectionString: process.env.DATABASE_URL  
})

// All of these arguments are optional.
const csvOptions = {
  // delimiter : '\t', // default is ,
  // endLine : '\n', // default is \n,
  // columns : ['orderId', 'customerId', 'item', 'quantity'], // by default read the first line and use values found as columns
  // columnOffset : 2, // default is 0
  // escapeChar : '"', // default is an empty string
  // enclosedChar : '"' // default is an empty string
}

/*
  Creating the orders table with a foreign key on customer_id, then using
  a filter+trigger to ignore invalid results would be the fastest and least memory
  intensive technique.

  ie:
    TABLE:

      CREATE TABLE IF NOT EXISTS
        orders(
          order_id INT PRIMARY KEY DEFAULT,
          customer_id INT NOT NULL,
          item TEXT NOT NULL,
          quantity INTEGER NOT NULL,
          FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON UPDATE CASCADE ON DELETE CASCADE
        );

    FUNCTION:

      CREATE FUNCTION order_insert_filter() RETURNS trigger
         LANGUAGE plpgsql AS
      $$BEGIN
         IF EXISTS(SELECT 1 FROM "Customers" WHERE "customer_id" = NEW."customer_id")
         THEN
            RETURN NEW;
         ELSE
            RAISE NOTICE 'Skipping row with "customer_id"=% and "Date"=%',
               NEW."customer_id", NEW."Date";
            RETURN NULL;
         END IF;
      END;$$;

    TRIGGER:

      CREATE TRIGGER order_insert_filter
        BEFORE INSERT ON "Orders" FOR EACH ROW
        EXECUTE PROCEDURE order_insert_filter();

  Yet the requirements specify that the data is parsed and filtered prior to insert.
  
  As such, we need to collect an array of customerIds from Customers table, 
  then something like a parsed stream, processing each line against a CustomerId Array.
 */

const customerIds = []

(async () => {

  const { rows } = await pool.query('SELECT customer_id FROM Customers')
  customerIds.push(rows)

})().catch(e => setImmediate(() => { throw e }))

/*
  Processing everything as streams should easily permit CSV Files in excess of the 1.5G limit,
  while also offering the capacity to read the upload file and process it in chunks instead of 
  writing the file.

  Would be far cleaner with Node > 10, with use of stream.pipelines() ...

  request `time` paramter offers full timer profiling options.
  Alternative would be the inclusion of `performance-now` module.

  es.map() should be replaced with a stream transform, else at least extended for data-checking,
  testing, and logging of timing results... for 6+ figure CSV files, backpressure safety 
  measures should be implemented.

  CSV Header should be tested to match Table columns, including order - COPY can be fast, yet brittle.
*/

pool.connect(function(err, client, done) {
  const pgStream = client.query(copyFrom(`COPY Orders FROM STDIN CSV`))
  
  pgStream.on('error', done)
  psStream.on('end', done)

  request
    .get({
      url: url,
      time: true
    })
    .pipe(csv.createStream(options))
    .pipe(es.split())
    .pipe(
      es.map(function (line, cb) {
        const { customerId } = line;

        // if current customer keep line
        if (customerIds.includes(customerId)) {
          cb(null, line)
        }
        // not current customer; drop data
        cb()
      })
    )
    .pipe(pgStream)
    .on('end', () => process.exit())
})