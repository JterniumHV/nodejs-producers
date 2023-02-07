const express = require('express');
const { Kafka } = require('kafkajs')
const fs = require("fs")
const PRUEBAS_MAGENTTA= require('../../models/PRUEBAS_MAGENTTA');

//------------------------------------------------------------------
//                      Send message tpc stock
//------------------------------------------------------------------

const SendMessageTpcStock= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const brokers=[];
    brokers.push(host);
    const total_test=       Number(body.total_test);
    const data =body.data;
    const name_test=        body.name_test;
    const type=             body.type;
    const action =          body.action;
    const number_seed=      Number(body.number_seed);

    console.log(`Total test: ${total_test}, Name test: ${name_test}, Type: ${type}, Action: ${action}`)

    const kafka = new Kafka({brokers:brokers})
    const producer = kafka.producer({
        allowAutoTopicCreation: false,
        transactionTimeout: 300000,
        //createPartitioner: Partitioners.DefaultPartitioner
    })
    await producer.connect()
    console.log(producer)
    try {

    
    const stock ={
        "data" : {
          "C_ID_MATERIAL" : 115829312,
          "CuentaStokAcanalados" : "NO",
          "C_UBICACION_PLANTA_DESC" : "Universidad",
          "N_PIEZAS" : 0,
          "C_CALIDAD" : "1ra",
          "C_NECESIDAD" : "003003981853",
          "C_UBICACION_ESTRATO" : "1",
          "C_DICTAMEN_DESC" : "Aceptado",
          "UTCTime" : 1673254297,
          "C_OFA" : "0503798435000010",
          "F_FECHA_PRODUCCION" : "20230108",
          "C_CLAVE" : "0300953421001",
          "C_PRODUCTO_COD" : "MP517396",
          "C_UBICACION_DEPOSITO" : "UNIUN",
          "C_ID_STOCK" : 1968635,
          "Pais" : "MX",
          "timestamp" : "2023-01-09T02:51:37.001Z",
          "C_LINEA_ANTERIOR" : "EMBG4_UNI",
          "C_LINEA_ACTUAL_DESC" : "ADM. PROD. TERMINADO",
          "C_UBICACION_LOTE" : "6",
          "N_ESPESOR" : 0.766,
          "n_id_ejecucion" : 50806140,
          "N_LARGO" : 820.466,
          "C_SOCIEDAD" : "TM01",
          "N_BIT_ZONA_DESPACHO" : 1,
          "C_GRADO" : "C091TM",
          "C_COLADA" : "2265530C11",
          "C_MERCADO_COD" : "MI",
          "C_ESTADO_POR_PRODUCTO" : "PT",
          "C_LINEA_ACTUAL" : "APT",
          "C_RESOLUCION_DESC" : "Sobre Orden",
          "C_LINEA_PROXIMA" : "APT",
          "C_UBICACION_PLANTA" : "UNI",
          "C_ESTRATEGIA" : "STH",
          "C_UBICACION_ZONA" : "UNIAE2ZPR",
          "C_MATERIAL" : "4A171453UG401",
          "C_FECHA_ULTIMO_MOVIMIENTO" : "20230109",
          "N_ANCHO" : 1224,
          "C_TIPO_MATERIAL" : "BOBINA",
          "C_ESTADO" : "MDIS",
          "N_PESO_NETO" : 5975,
          "C_UBICACION_FILA" : "30",
          "c_llave" : 115829312
        },
        "domain" : "Stock",
        "trx" : "Inventario"
      }

      let estado= "SI"
        if(action=="baja"){
            estado= "NO"
        }
    
      const topicMessages = [];
      console.log(`Interval: ${number_seed} to: ${total_test+number_seed}`)
      const messagesJSON=[]
        
        const messagesCabecera=[]
        console.log("----- creating stock messages-----")
        for(let i=number_seed; i<total_test+number_seed; i++){
            const dataSend = stock;
            dataSend.data.C_ID_MATERIAL=i+1;
            dataSend.data.C_MATERIAL=`${i+1}`;
            dataSend.data.c_llave=i+1;
            dataSend.data.CuentaStokAcanalados=estado;

            //console.log("---------------------- linea ", i+1 , " -------------------------------------")
            //console.log(dataSend)

            messagesCabecera.push({value: JSON.stringify(dataSend)});
            messagesJSON.push(JSON.stringify(dataSend));

            // JSONresult.push(data);
            // await producer.send({
            // topic: 'tpc-nca-stock',
            // messages: [
            //     { value: JSON.stringify(data)  },
            // ],
            // })
        }

        console.log("----- finished stock messages-----")
        console.log("")
        console.log("")
        //topicMessages.push({topic:"tpc-nca-stock", messages: messagesCabecera});
        topicMessages.push({topic:"tpc-nca-stock-test", messages: messagesCabecera});


        const initial= new Date()
        console.log("----- sending messages-----")
        console.log("Init time: ", initial)
        
        console.log("Enviando bloque de datos")
        await producer.sendBatch({topicMessages})

        console.log("desconectando producer...")
        await producer.disconnect()
        const end= new Date()
        console.log("End time: ", end)
        console.log("producer desconectado")
        console.log("")
        console.log("")

        const SavePrueba = new PRUEBAS_MAGENTTA();
        SavePrueba.Start_Time = initial;
        SavePrueba.End_Time = end;
        SavePrueba.Name_Test =name_test;
        SavePrueba.Total_Test= total_test;
        SavePrueba.Type_Events= type.toString();
        SavePrueba.Operation_Time= `${(end.getTime()-initial.getTime())/(1000)} - Seconds`;
        SavePrueba.Host = host;

        console.log(SavePrueba);

        SavePrueba.save();

        // console.log("Generando JSON de mensajes enviados....")
        //     let jsonData = JSON.stringify(messagesJSON);
        //     //console.log(jsonData)
        //     fs.writeFile('./Data/stock.json', jsonData, (error)=>{
        //         if(error){
        //             console.log(`Error: ${error}`);
        //         }else{
        //             console.log(`Archivo guardado correctamente`);
        //         }
        //     });


            res.status(200).json({
                ok:true,
                msg:'Message OK',
                data: topicMessages
            })
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-stock'
        })
    }

}







//------------------------------------------------------------------
//                      Send message tpc stock looper
//------------------------------------------------------------------

const SendMessageTpcStockLooper= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const brokers=[];
    brokers.push(host);
    const total_test=       Number(body.total_test);
    const data =body.data;
    const name_test=        body.name_test;
    const type=             body.type;
    const action =          body.action;
    const number_seed=      Number(body.number_seed);
    const loops =           Number(body.loops)

    console.log(`Total test: ${total_test}, Name test: ${name_test}, Type: ${type}, Action: ${action}, Loops: ${loops}`)

    

    const kafka = new Kafka({brokers:brokers})
    const producer = kafka.producer({
        allowAutoTopicCreation: false,
        transactionTimeout: 300000,
        //createPartitioner: Partitioners.DefaultPartitioner
    })
    await producer.connect()
    console.log(producer)
    
    try {

    
    const stock ={
        "data" : {
          "C_ID_MATERIAL" : 115829312,
          "CuentaStokAcanalados" : "NO",
          "C_UBICACION_PLANTA_DESC" : "Universidad",
          "N_PIEZAS" : 0,
          "C_CALIDAD" : "1ra",
          "C_NECESIDAD" : "003003981853",
          "C_UBICACION_ESTRATO" : "1",
          "C_DICTAMEN_DESC" : "Aceptado",
          "UTCTime" : 1673254297,
          "C_OFA" : "0503798435000010",
          "F_FECHA_PRODUCCION" : "20230108",
          "C_CLAVE" : "0300953421001",
          "C_PRODUCTO_COD" : "MP517396",
          "C_UBICACION_DEPOSITO" : "UNIUN",
          "C_ID_STOCK" : 1968635,
          "Pais" : "MX",
          "timestamp" : "2023-01-09T02:51:37.001Z",
          "C_LINEA_ANTERIOR" : "EMBG4_UNI",
          "C_LINEA_ACTUAL_DESC" : "ADM. PROD. TERMINADO",
          "C_UBICACION_LOTE" : "6",
          "N_ESPESOR" : 0.766,
          "n_id_ejecucion" : 50806140,
          "N_LARGO" : 820.466,
          "C_SOCIEDAD" : "TM01",
          "N_BIT_ZONA_DESPACHO" : 1,
          "C_GRADO" : "C091TM",
          "C_COLADA" : "2265530C11",
          "C_MERCADO_COD" : "MI",
          "C_ESTADO_POR_PRODUCTO" : "PT",
          "C_LINEA_ACTUAL" : "APT",
          "C_RESOLUCION_DESC" : "Sobre Orden",
          "C_LINEA_PROXIMA" : "APT",
          "C_UBICACION_PLANTA" : "UNI",
          "C_ESTRATEGIA" : "STH",
          "C_UBICACION_ZONA" : "UNIAE2ZPR",
          "C_MATERIAL" : "4A171453UG401",
          "C_FECHA_ULTIMO_MOVIMIENTO" : "20230109",
          "N_ANCHO" : 1224,
          "C_TIPO_MATERIAL" : "BOBINA",
          "C_ESTADO" : "MDIS",
          "N_PESO_NETO" : 5975,
          "C_UBICACION_FILA" : "30",
          "c_llave" : 115829312
        },
        "domain" : "Stock",
        "trx" : "Inventario"
      }

      let estado= "SI"
        if(action=="baja"){
            estado= "NO"
        }
    let calculate_seed=number_seed;

    const initial= new Date()
        console.log("----- sending messages-----")
        console.log("Init time: ", initial)
    for(let lp=0;lp<loops; lp++){
      const calculate_total= calculate_seed+total_test;
      const topicMessages = [];
      console.log(`Interval: ${calculate_seed} to: ${calculate_total}`)
      const messagesJSON=[]
        
        const messagesCabecera=[]
        console.log("----- creating stock messages-----")
        const cont=0;

        
        for(let i=calculate_seed; i<calculate_total; i++){
            const dataSend = stock;
            dataSend.data.C_ID_MATERIAL=i+1;
            dataSend.data.C_MATERIAL=`${i+1}`;
            dataSend.data.c_llave=i+1;
            dataSend.data.CuentaStokAcanalados=estado;

            //console.log("---------------------- linea ", i+1 , " -------------------------------------")
            //console.log(dataSend)

            messagesCabecera.push({value: JSON.stringify(dataSend)});
            messagesJSON.push(JSON.stringify(dataSend));

            // JSONresult.push(data);
            // await producer.send({
            // topic: 'tpc-nca-stock',
            // messages: [
            //     { value: JSON.stringify(data)  },
            // ],
            // })
        }

        console.log("----- finished stock messages-----")
        console.log("")
        console.log("")
        //topicMessages.push({topic:"tpc-nca-stock", messages: messagesCabecera});
        topicMessages.push({topic:"tpc-nca-stock-test", messages: messagesCabecera});


        
        
        console.log("Enviando bloque de datos")
        await producer.sendBatch({topicMessages})

       
        
       
        

       

        // console.log("Generando JSON de mensajes enviados....")
        //     let jsonData = JSON.stringify(messagesJSON);
        //     //console.log(jsonData)
        //     fs.writeFile('./Data/stock.json', jsonData, (error)=>{
        //         if(error){
        //             console.log(`Error: ${error}`);
        //         }else{
        //             console.log(`Archivo guardado correctamente`);
        //         }
        //     });
        calculate_seed=calculate_seed+total_test;
    }
    console.log("desconectando producer...")
    await producer.disconnect()
    console.log("producer desconectado")
    console.log("")
    console.log("")
    const end= new Date()
    console.log("End time: ", end)
    const SavePrueba = new PRUEBAS_MAGENTTA();
    SavePrueba.Start_Time = initial;
    SavePrueba.End_Time = end;
    SavePrueba.Name_Test =name_test;
    SavePrueba.Total_Test= total_test;
    SavePrueba.Type_Events= type.toString();
    SavePrueba.Operation_Time= `${(end.getTime()-initial.getTime())/(1000)} - Seconds`;
    SavePrueba.Host = host;

    console.log(SavePrueba);

    await SavePrueba.save();
            res.status(200).json({
                ok:true,
                msg:'Message OK',
                //data: topicMessages
            })
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-stock'
        })
    }

}


const SendMessageTpcStockIterator= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const action =          body.action;
    const number_seed=      Number(body.number_seed);
    const brokers=[];
    brokers.push(host);
    const total_test=       Number(body.total_test);

    const kafka = new Kafka({brokers:brokers})
    console.log(kafka)
    console.log("creando producer")
        const producer = kafka.producer({
                allowAutoTopicCreation: false,
                transactionTimeout: 300000,
                acks: 0,
            })
            await producer.connect()
    try {
        console.log(producer)

        const stock ={
        "data" : {
          "C_ID_MATERIAL" : 115829312,
          "CuentaStokAcanalados" : "NO",
          "C_UBICACION_PLANTA_DESC" : "Universidad",
          "N_PIEZAS" : 0,
          "C_CALIDAD" : "1ra",
          "C_NECESIDAD" : "003003981853",
          "C_UBICACION_ESTRATO" : "1",
          "C_DICTAMEN_DESC" : "Aceptado",
          "UTCTime" : 1673254297,
          "C_OFA" : "0503798435000010",
          "F_FECHA_PRODUCCION" : "20230108",
          "C_CLAVE" : "0300953421001",
          "C_PRODUCTO_COD" : "MP517396",
          "C_UBICACION_DEPOSITO" : "UNIUN",
          "C_ID_STOCK" : 1968635,
          "Pais" : "MX",
          "timestamp" : "2023-01-09T02:51:37.001Z",
          "C_LINEA_ANTERIOR" : "EMBG4_UNI",
          "C_LINEA_ACTUAL_DESC" : "ADM. PROD. TERMINADO",
          "C_UBICACION_LOTE" : "6",
          "N_ESPESOR" : 0.766,
          "n_id_ejecucion" : 50806140,
          "N_LARGO" : 820.466,
          "C_SOCIEDAD" : "TM01",
          "N_BIT_ZONA_DESPACHO" : 1,
          "C_GRADO" : "C091TM",
          "C_COLADA" : "2265530C11",
          "C_MERCADO_COD" : "MI",
          "C_ESTADO_POR_PRODUCTO" : "PT",
          "C_LINEA_ACTUAL" : "APT",
          "C_RESOLUCION_DESC" : "Sobre Orden",
          "C_LINEA_PROXIMA" : "APT",
          "C_UBICACION_PLANTA" : "UNI",
          "C_ESTRATEGIA" : "STH",
          "C_UBICACION_ZONA" : "UNIAE2ZPR",
          "C_MATERIAL" : "4A171453UG401",
          "C_FECHA_ULTIMO_MOVIMIENTO" : "20230109",
          "N_ANCHO" : 1224,
          "C_TIPO_MATERIAL" : "BOBINA",
          "C_ESTADO" : "MDIS",
          "N_PESO_NETO" : 5975,
          "C_UBICACION_FILA" : "30",
          "c_llave" : 115829312
        },
        "domain" : "Stock",
        "trx" : "Inventario"
      }
        
       
        
        //const topicMessages = [];

        let estado= "SI"
        if(action=="baja"){
            estado= "NO"
        }

       
            const messages=[]
            console.log("----- creating otd v1 messages-----")
            for(let i=number_seed; i<total_test+number_seed; i++){
                const dataSend = stock;
                dataSend.data.C_ID_MATERIAL=i+1;
                dataSend.data.C_MATERIAL=`${i+1}`;
                dataSend.data.c_llave=i+1;
                dataSend.data.CuentaStokAcanalados=estado;
                
                //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                //console.log(a_mad)
                //console.log(dataSend)
                messages.push({value: JSON.stringify(dataSend)});
                await producer.send({
                topic: 'tpc-nca-stock-test',
                acks: 0,
                messages: [
                    { value: JSON.stringify(dataSend)  },
                    //{ value: JSON.stringify(dataSend)  },
                ],
                })
            }
            // console.log("----- finished otd messages-----")
            // console.log("")
            // console.log("")
            // topicMessages.push({topic:"tpc-nca-apt-ordenestraslado", messages: messages})
            
            // return;
            // console.log("Enviando bloque de datos")
            // await producer.sendBatch({topicMessages})

            console.log("desconectando producer")
            await producer.disconnect()
                res.status(200).json({
                    ok:true,
                    msg:'Message OK',
                    //data: topicMessages
                    data: messages
                })
             
        
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-nca-ordenestraslado'
        })
    }

}

module.exports={
    SendMessageTpcStock,
    SendMessageTpcStockLooper,
    SendMessageTpcStockIterator
}