const app= require ('./app');
const { dbConnection } = require('./database/config');


async function main(){
    dbConnection();
    const port = 5000;
    await app.listen(port,"0.0.0.0")
    console.log(`server on port: ${port}`)
}
main();

