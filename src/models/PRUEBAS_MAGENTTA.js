const {Schema , model} = require ('mongoose');

const PRUEBAS_MAGENTTASchema = Schema({
Start_Time:{
    type: Date,
    require: true
},
End_Time:{
    type: Date,
    require: true
},
Operation_Time:{
    type: String,
    require: true
},
Name_Test:{
    type: Object,
    require: true
},
Total_Test:{
    type: Number,
    require: true
},
Type_Events:{
    type: String,
    require: true
},
Host:{
    type: String,
    require: true
}

});

module.exports= model('PRUEBAS_MAGENTTA', PRUEBAS_MAGENTTASchema, 'PRUEBAS_MAGENTTA');