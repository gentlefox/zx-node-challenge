// Import required modules
const fs = require('fs-extra')
const { Pool } = require('pg')
const csv = require('csv-stream')
const request = require('request')
const copyFrom = require('pg-copy-streams').from
const dotenv = require('dotenv')

dotenv.config()

// Setting file url
const inputFileUrl = 'https://someurl.com/path/to/orders.csv'

// target table
const table = 'orders'

const pool = new Pool({
  connectionString: process.env.DATABASE_URL  
})

const client = await pool.connect()

const csvOptions = {
  delimiter : ',',
  endLine : '\n',
  columns : ['orderId', 'customerId', 'item', 'quantity'], // by default read the first line and use values found as columns
  columnOffset : 0, // default is 0
  escapeChar : '"', // default is an empty string
  enclosedChar : '"' // default is an empty string
}
 
const executeQuery = (targetTable) => {
  const csvStream = csv.createStream(csvOptions)
  const stream = client.query(copyFrom(`COPY Orders FROM STDIN CSV HEADER`))
  const fileStream = request(inputFileUrl).pipe(csvStream)
  
  fileStream.on('error', (error) =>{
    console.log(`Error in creating read stream ${error}`)
  })
  stream.on('error', (error) => {
    console.log(`Error in creating stream ${error}`)
  })
  stream.on('end', () => {
    console.log(`Completed loading data into ${targetTable}`)
    client.end()
  })

  fileStream.pipe(stream);
}

// Execute the function
executeQuery(table)
