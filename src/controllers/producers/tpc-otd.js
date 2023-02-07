const express = require('express');
const { Kafka } = require('kafkajs')
const fs = require("fs")
const PRUEBAS_MAGENTTA= require('../../models/PRUEBAS_MAGENTTA');

//------------------------------------------------------------------
//                      Send message tpc otd
//------------------------------------------------------------------

const SendMessageTpcOTD= async (req, res = express.response) =>{

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
        
        const otd={
            "data" : {
              "Orden" : {
                "id_stock" : "2168902",
                "c_cod_planta_dest" : "BVSL",
                "c_cod_zona_orig" : "null",
                "c_cod_deposito_dest" : "BVSLP",
                "f_entrega" : "2023-01-12T00:00:00.000Z",
                "c_estado" : "ABI",
                "_nca_log_id" : "2023-01-10T17:49:38.000Z",
                "c_cod_planta_orig" : "CHU",
                "c_id_orden_cabecera" : "15156152",
                "Items" : [ {
                  "c_id_tipo_estrategia" : "MTC",
                  "n_espesor" : "4.25",
                  "n_rango_hasta" : "4.5",
                  "c_prodto" : "MP534390",
                  "c_necesidad" : "null",
                  "q_despachada" : "33.08",
                  "n_ancho" : "241.0",
                  "q_solicitada" : "43.745",
                  "n_prioridad" : "5.0",
                  "c_posicion_orden" : "2.0",
                  "n_rango_desde" : "2.2"
                } ],
                "c_cod_zona_dest" : "null",
                "c_cod_almacen_orig" : "null",
                "c_llave" : "15156152",
                "c_cod_almacen_dest" : "BVMSLP"
              }
            },
            "domain" : "APT",
            "trx" : "OrdenesTraslado"
          }

        let estado= "ABI"
        if(action=="baja"){
            estado= "BAJ"
        }

        const topicMessages = [];
        console.log(`Interval: ${number_seed} to: ${total_test+number_seed}`)
        const messagesJSON=[]
        
        const messagesCabecera=[]

        console.log("----- creating OTD messages-----")
        for(let i=number_seed; i<total_test+number_seed; i++){
            const dataSend = otd;
            dataSend.data.Orden.c_id_orden_cabecera=`${i+1}`;
            dataSend.data.Orden.c_estado=estado;
            dataSend.data.Orden.c_llave= `${i+1}`;

            //console.log("---------------------- linea ", i+1 , " -------------------------------------")
            //console.log(dataSend)

            messagesCabecera.push({value: JSON.stringify(dataSend)});
            messagesJSON.push(JSON.stringify(dataSend));


            // data.data.c_id_orden_cabecera=i+1
            // console.log("---------------------- linea ", i+1 , " -------------------------------------")
            // console.log(data)
            // JSONresult.push(data);
            // await producer.send({
            // topic: 'tpc-nca-apt-ordenestraslado',
            // messages: [
            //     { value: JSON.stringify(data)  },
            // ],
            // })
        }

        console.log("----- finished OTD messages-----")
        console.log("")
        console.log("")
        topicMessages.push({topic:"tpc-nca-apt-ordenestraslado", messages: messagesCabecera});


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
        //     fs.writeFile('./Data/otd.json', jsonData, (error)=>{
        //         if(error){
        //             console.log(`Error: ${error}`);
        //         }else{
        //             console.log(`Archivo guardado correctamente`);
        //         }
        //     });

        await producer.disconnect()
            res.status(200).json({
                ok:true,
                msg:'Message OK',
                data: topicMessages
            })
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-nca-apt-ordenestraslado'
        })
    }

}



const SendMessageTpcOtdByPosition= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const brokers=[];
    brokers.push(host);
    const total_test=body.total_test;
    //const data =body.data;
    //console.log(data)
    
    const { Partitioners } = require('kafkajs')
    const kafka = new Kafka({brokers:brokers})
    console.log(kafka)
    console.log("creando producer")
        const producer = kafka.producer({
                allowAutoTopicCreation: false,
                transactionTimeout: 300000,
                createPartitioner: Partitioners.DefaultPartitioner
            })
            await producer.connect()
    try {
        console.log(producer)

        const otd={
            
            
                "domain" : "APT",
                "trx" : "OrdenesTraslado",
                "data" : {
                  "Orden" : {
                    "_nca_log_id" : "2022-08-04T02:15:29.000357Z",
                    "c_llave" : "14321722",
                    "c_id_orden_cabecera" : "14321722",
                    "c_cod_planta_orig" : "CHU",
                    "c_cod_planta_dest" : "JUV",
                    "c_cod_deposito_dest" : "JUVGR2",
                    "c_cod_almacen_dest" : "JUVMR2",
                    "c_cod_zona_dest" : "JUVAG2ZG2",
                    "f_entrega" : "2022-07-31T00:00:00Z",
                    "c_estado" : "ABI",
                    "Items" : [ {
                      "c_posicion_orden" : "1.0",
                      "q_solicitada" : "105.369",
                      "q_despachada" : "67.495",
                      "n_espesor" : "0.3429",
                      "n_ancho" : "1038.0",
                      "c_prodto" : "MP522781",
                      "c_id_tipo_estrategia" : "STO",
                      "c_necesidad" : "",
                      "n_prioridad" : "5.0",
                      "n_rango_desde" : "13.0",
                      "n_rango_hasta" : "18.0"
                    } ]
                  }
                }
              
              
        }
        
       
        
        const topicMessages = [];

       
            const messages=[]
            console.log("----- creating otd v1 messages-----")
            for(let i=0; i< total_test; i++){
                const dataSend = otd;
                dataSend.data.Orden.Items[0].c_posicion_orden=`${i+1}.0`;
                
                console.log("---------------------- linea ", i+1 , " -------------------------------------")
                //console.log(a_mad)
                console.log(dataSend)
                messages.push({value: JSON.stringify(dataSend)});
            //    await  producer.send({
            //     topic: 'tpc-nca-viajes',
            //     messages: [
            //         { value: JSON.stringify(dataSend)  },
            //         { value: JSON.stringify(dataSend)  },
            //     ],
            //     })
            }
            console.log("----- finished otd messages-----")
            console.log("")
            console.log("")
            topicMessages.push({topic:"tpc-nca-apt-ordenestraslado", messages: messages})
            
    
            console.log("Enviando bloque de datos")
            await producer.sendBatch({topicMessages})

            console.log("desconectando producer")
            await producer.disconnect()
                res.status(200).json({
                    ok:true,
                    msg:'Message OK',
                    data: topicMessages
                })
             
        
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-ofa-id-stock'
        })
    }

}

module.exports={
    SendMessageTpcOTD,
    SendMessageTpcOtdByPosition
}