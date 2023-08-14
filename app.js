const express = require('express');
const { Pool } = require('pg'); // Import the PostgreSQL driver

// create an instance of the express application 
const app = express(); 


//Database connection setup 

const pool = new Pool({
    user: 'doadmin',
    password: 'AVNS_rbiTyQ_BS7wAYbXjFhV',
    host: 'jobjack-tech-assessments-do-user-3965150-0.b.db.ondigitalocean.com',
    port: 25060,
    database: 'tech_assess_keegan',
    ssl: {
      rejectUnauthorized: false // Only needed if using self-signed certificates
    }
  });
  

//ETL End Point 
app.get('/start-etl', async (req, res) => {
  try {
    // Extract data from "jack_location" table
    const query = 'SELECT * FROM jack_location';
    const result = await pool.query(query);

    // Transform and aggregate data
    const dataRows = result.rows;
    const aggregatedData = {};

    dataRows.forEach(row => {
      const location = dataRows.full_location; // Location field
      const countField = dataRows.sign_up_date; // field to count
   

      // Initialize count for each location if it doesn't exist
      if (!aggregatedData[location]) {
        aggregatedData[location] = 0;
      }

      // Increment the count for the location
      aggregatedData[location] += countField;
    });

    // Load aggregated data into "jack_location_density" table
    for (const location in aggregatedData) {

      let count = aggregatedData[location];
      //if count is nan then make it 0 else count it. 
      count = Number.isNaN(count) ? 0 : count;

      const suburb = dataRows.suburb ;
      const city = dataRows.city;
      const province = dataRows.province;
      
      // Insert the aggregated data into the "jack_location_density" table
      const insertQuery = `
        INSERT INTO jack_location_density (location, count, suburb, city, province)
        VALUES ($1, $2, $3, $4 , $5)
         
      `; //
      await pool.query(insertQuery, [location, count, suburb, city, province]);
    }

    res.json({ message: 'ETL process completed successfully' });
  } catch (error) {
    console.error('Error during ETL:', error);
    res.status(500).json({ error: 'An error occurred during ETL' });
  }
});



  
  //start the server 
  const port = 3000;
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
