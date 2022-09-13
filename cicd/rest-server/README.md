### Prerequisite: nodejs > 7.5, npm > 4.0, mysql > 5.6

* * *

### Production mode
1. Download npm packages:<br/>
    `npm install`

2. Start server:<br/>
    `npm start`
    * Loading kafka data into mysql starts at the same time
    * Config should be set and checked in the config-override.js before that.

### Development mode:
1. Download npm packages:<br/>
    `npm install`

2. Initialize database:<br/>
    `npm run init-db `

3. Generate fake data with data size:<br/>
    `npm run fake datasize`

4. Run the Rest Server in the dev mode:<br/>
    `npm run dev`
    * Will NOT load kafka data into mysql at the same time</br></br>

5. Starting loading kafka data into mysql:<br/>
    `npm run kafka`<br/>

6. Run testing by mocha:<br/>
    `npm test`
    * server must be start before testing. "npm start" or "npm run dev"
