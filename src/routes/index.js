const {Router} = require('express');
//const { GetAllOfas } = require('../controllers/OfaController');

 const {SendMessageTpcNcaViajes, SendMessageTpcNcaViajesBaja } = require("../controllers/producers/tpc-nca-viajes");
const { SendMessageTpcOFA } = require('../controllers/producers/tpc-ofa');
const { SendMessageTpcOTD, SendMessageTpcOtdByPosition } = require('../controllers/producers/tpc-otd');
const { SendMessageTpcStockLooper, SendMessageTpcStock, SendMessageTpcStockIterator } = require('../controllers/producers/tpc-stock');
const router = Router();

//router.get('/',(req, res)=> res.json({message: "hello world"}))

//router.get('/ofa',GetAllOfas)

/*
    Routes
*/

//viajes
console.log("all routes")
router.post('/api/tpc-nca/viajes/v1/producer', SendMessageTpcNcaViajes)
router.post('/api/tpc-nca/viajes/v1/producer/baja', SendMessageTpcNcaViajesBaja);

//stock
router.post('/api/tpc-nca/stock/producer', SendMessageTpcStock);
router.post('/api/tpc-nca/stock/producer/looper', SendMessageTpcStockLooper);
router.post('/api/tpc-nca/stock/producer/iterator', SendMessageTpcStockIterator);


//otd
router.post('/api/tpc-nca/otd/producer', SendMessageTpcOTD);
router.post('/api/tpc-nca/otd/producer/position', SendMessageTpcOtdByPosition);

//otd
router.post('/api/tpc-nca/ofa/producer', SendMessageTpcOFA);


console.log(router)
module.exports = router;