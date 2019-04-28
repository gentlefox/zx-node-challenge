const { Pool } = require('pg')
const dotenv = require('dotenv')
const faker  = require('faker')
const stringify = require('csv-stringify')
const assert = require('assert')

dotenv.config()

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

pool.on('connect', () => {
  console.log('connected to the db')
})

/**
 * Create Customers Table
 */
const createCustomersTable = () => {
  const queryText = `
    CREATE EXTENSION pgcrypto IF NOT EXISTS;

    CREATE TABLE IF NOT EXISTS
      customers(
        customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        first_name VARCHAR(128) NOT NULL,
        last_name VARCHAR(128) NOT NULL
      );
    `

  pool.query(queryText)
    .then((res) => {
      console.log(res)
      pool.end()
    })
    .catch((err) => {
      console.log(err)
      pool.end()
    });
}

/**
 * Create Orders Table
 */
const createOrdersTable = () => {
  const queryText = `
    CREATE EXTENSION pgcrypto IF NOT EXISTS;

    CREATE TABLE IF NOT EXISTS
      orders(
        order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_id UUID NOT NULL,
        item TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON UPDATE CASCADE ON DELETE CASCADE
      );
    `

  pool.query(queryText)
    .then((res) => {
      console.log(res)
      pool.end()
    })
    .catch((err) => {
      console.log(err)
      pool.end()
    });
}

/**
 * Drop Customers Table
 */
const dropCustomersTable = () => {
  // NB: TRUNCATE a better option for speed... 
  const queryText = 'DROP TABLE IF EXISTS customers returning *'

  pool.query(queryText)
    .then((res) => {
      console.log(res)
      pool.end()
    })
    .catch((err) => {
      console.log(err)
      pool.end()
    })
}

/**
 * Drop Orders Table
 */
const dropOrdersTable = () => {
  const queryText = 'DROP TABLE IF EXISTS orders returning *';

  pool.query(queryText)
    .then((res) => {
      console.log(res)
      pool.end()
    })
    .catch((err) => {
      console.log(err)
      pool.end()
    });
}

/**
 * Create All Tables
 */
const createAllTables = () => {
  createCustomersTable()
  createOrdersTable()
}

/**
 * Drop All Tables
 */
const dropAllTables = () => {
  dropCustomersTable()
  dropOrdersTable()
}


/**
 * Create Customers (dummy data creation)
 */
// const createCustomers = async (records = 50) => {

//   const customers = []
//   const timestamp = Date.now()
//   const filename = `./build/customers.json`

//   for(var i = records; i >= 0; i--) {
//     customers.push({
//       customerId: faker.random.uuid(),
//       firstName: faker.name.firstName(),
//       lastName: faker.name.lastName()
//     })
//   }

//   const client = await pool.connect()
//   await fs.outputJson(filename, JSON.stringify(customers))

//   console.log(`Customer JSON File created as ${filename}`)

//   // option 1
//   const stream = await client.query(copyFrom(`COPY customers FROM STDIN`))
//   // option 2
//   const stream = await client.query(copyFrom(`COPY customers FROM ${filename}`))
//   const fileStream = fs.createReadStream(filename)
// }

/**
 * Create Orders (CSV File)
 */
// const createOrders = (records = 300) => {

//   const timestamp = Date.now()
//   const filename = `./build/orders_${timestamp}.csv`

//   fs.readJson('./build/customers.json')
//     .then((customers) => {
//       // get generated customer uuids for "actual customer" results
//       const customerIds = customers.map((customer) => customer.customerId)
//       const orders = []

//       // every 10th record will have an unlisted customerId
//       for(var i = records; i >= 0; i--) {
//         orders.push({
//           orderId: faker.random.uuid(),
//           customerId: i % 10 ? faker.random.uuid() : customerIds[Math.floor(Math.random()*customerIds.length)],
//           item: faker.commerce.product(),
//           quantity: faker.random.number()
//         })
//       }

//       return orders;
//     })
//     .then((orders) => {
//       stringify(orders, {
//         columns: 4,
//         rowDelimiter: '\n',
//         header: true,
//         columns: ['orderId', 'customerId', 'item', 'quantity']
//       }, function(err, ordersCSV) {
//         fs.outputFile(filename, ordersCSV)
//           .then(() => {
//             console.log(`Orders CSV File created as ${filename}`)
//           })
//           .catch((err) => console.log(err))
//       })
//     })
//     .catch((err) => {
//       console.error(err)
//     })
// }

pool.on('remove', () => {
  console.log('client removed')
  process.exit(0)
})

module.exports = {
  createCustomersTable,
  createOrdersTable,
  dropCustomersTable,
  dropOrdersTable,
  createAllTables,
  dropAllTables
};