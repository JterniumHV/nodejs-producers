const express = require('express');
const { Kafka } = require('kafkajs')
const fs = require("fs")
const PRUEBAS_MAGENTTA= require('../../models/PRUEBAS_MAGENTTA');


//------------------------------------------------------------------
//                      Send message tpc ofa
//------------------------------------------------------------------

const SendMessageTpcNcaViajes= async (req, res = express.response) =>{

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

        const viajeAlta={
            "data" : {
              "Viaje" : {
                "COD_DEPOSITO" : "GUEPL",
                "F_FECHA_HORA_PRESENTACION" : "1999-01-01 00:00:00.0000000",
                "Orden" : [ {
                  "C_ID_ORDEN_CABECERA" : "15153321",
                  "ESTRATEGIA_CLAVE" : "0300957309000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642399"
                  } ],
                  "F_ENTREGA" : "2023-01-31T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15120061",
                  "ESTRATEGIA_CLAVE" : "0300923208000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642397",
                    "Q_CARGA_PROG" : "10.179"
                  } ],
                  "F_ENTREGA" : "2023-01-08T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15120035",
                  "ESTRATEGIA_CLAVE" : "0300851866000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642395",
                    "Q_CARGA_PROG" : "17.983"
                  } ],
                  "F_ENTREGA" : "2023-01-02T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15120055",
                  "ESTRATEGIA_CLAVE" : "0300924071000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642396",
                    "Q_CARGA_PROG" : "17.758"
                  } ],
                  "F_ENTREGA" : "2023-01-08T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15153241",
                  "ESTRATEGIA_CLAVE" : "0300891618000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642398",
                    "Q_CARGA_PROG" : "18.882"
                  } ],
                  "F_ENTREGA" : "2023-01-09T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15163315",
                  "ESTRATEGIA_CLAVE" : "0300977863000010002001473596SKP4_CHU",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "REE",
                    "ID_ORDEN_PROG" : "115640221",
                    "Q_CARGA_PROG" : "17.68"
                  } ],
                  "F_ENTREGA" : "2023-01-11T09:32:50.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "SON"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15163318",
                  "ESTRATEGIA_CLAVE" : "0300989155000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "REE",
                    "ID_ORDEN_PROG" : "115640233",
                    "Q_CARGA_PROG" : "21.54"
                  } ],
                  "F_ENTREGA" : "2023-01-11T09:32:55.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15163647",
                  "ESTRATEGIA_CLAVE" : "0301008361000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642406"
                  } ],
                  "F_ENTREGA" : "2023-01-16T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "ATW"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15163653",
                  "ESTRATEGIA_CLAVE" : "0301008364000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642407"
                  } ],
                  "F_ENTREGA" : "2023-01-11T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "ATW"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15158723",
                  "ESTRATEGIA_CLAVE" : "0301007142000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115599373",
                    "Q_CARGA_PROG" : "1.227"
                  }, {
                    "C_POSICION_ORDEN" : "3",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115599375",
                    "Q_CARGA_PROG" : "1.438"
                  } ],
                  "F_ENTREGA" : "2023-01-10T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "ATW"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15162025",
                  "ESTRATEGIA_CLAVE" : "0300986416000110002001476471TR_GUE_CHU",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642393",
                    "Q_CARGA_PROG" : "16.939"
                  } ],
                  "F_ENTREGA" : "2023-01-11T03:08:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "SON"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15162979",
                  "ESTRATEGIA_CLAVE" : "0503838815000010003003992639TR_GUE_CHU",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642394",
                    "Q_CARGA_PROG" : "21.0"
                  } ],
                  "F_ENTREGA" : "2023-01-13T12:50:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "SON"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15162764",
                  "ESTRATEGIA_CLAVE" : "0300951944000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642400",
                    "Q_CARGA_PROG" : "19.575"
                  } ],
                  "F_ENTREGA" : "2023-01-15T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                }, {
                  "C_ID_ORDEN_CABECERA" : "15163516",
                  "ESTRATEGIA_CLAVE" : "0300951945000010",
                  "Items" : [ {
                    "C_POSICION_ORDEN" : "1",
                    "C_ESTADO_PROG" : "ELB",
                    "ID_ORDEN_PROG" : "115642401",
                    "Q_CARGA_PROG" : "19.605"
                  } ],
                  "F_ENTREGA" : "2023-01-15T00:00:00.000Z",
                  "C_ID_TIPO_ESTRATEGIA" : "MTO"
                } ],
                "C_SOCIEDAD" : "TM01",
                "FECHA_VIAJE" : "null",
                "F_FECHA_ULT_MOD" : "2023-01-11T10:34:20.000Z",
                "C_ID_MEDIO_TRANSPORTE" : "CA",
                "C_ID_PLANTA_DEST" : "null",
                "C_ID_PERMISO_CIRCULACION" : "0",
                "C_COD_FAMILIA_CAMION" : "",
                "C_ID_PLANTA" : "157700851",
                "UTCTime" : "1673454859",
                "NRO_VIAJE" : "48632548",
                "COD_PLANTA" : "GUE",
                "M_CRITICO" : "N",
                "C_ESTADO" : "ELA",
                "Pais" : "MX",
                "F_FECHA_ALTA" : "2023-01-11T10:34:06.000Z",
                "timestamp" : "2023-01-11T10:34:19.001Z"
              }
            },
            "domain" : "APT",
            "trx" : "GeneracionViaje"
          }

          const viajeBaja= {"data":{"NRO_VIAJE":46388500,"C_ESTADO":"BAV"},"domain":"APT","trx":"GeneracionViaje"}
        
          let estado= "PRO"
          if(action=="baja"){
              estado= "BAJ"
          }
        
          const topicMessages = [];
          console.log(`Interval: ${number_seed} to: ${total_test+number_seed}`)
          const messagesJSON=[]

          
          if(action=="alta"){
          const messagesCabecera=[]
            console.log("----- creating 'viajes alta' messages-----")
            for(let i=number_seed; i<total_test+number_seed; i++){
                const dataSend = viajeAlta;
                dataSend.data.Viaje.NRO_VIAJE=`${i+1}`;
                dataSend.data.Viaje.C_ESTADO= estado;
                //dataSend.data.C_POSICION_ORDEN=1;
                //dataSend.data.ancho_madre=`${5000+i}`;
                
                console.log("---------------------- linea ", i+1 , " -------------------------------------")
                //console.log(a_mad)
                //console.log(dataSend)

                messagesCabecera.push({value: JSON.stringify(dataSend)});
                messagesJSON.push(JSON.stringify(dataSend));
                //messages.push({value: JSON.stringify(dataSend)});
            //    await  producer.send({
            //     topic: 'tpc-nca-viajes',
            //     messages: [
            //         { value: JSON.stringify(dataSend)  },
            //         { value: JSON.stringify(dataSend)  },
            //     ],
            //     })
            }
            console.log("----- finished 'viajes alta' messages-----")
            console.log("")
            console.log("")
            topicMessages.push({topic:"tpc-nca-viajes", messages: messagesCabecera});
        }

        if(action=="baja"){
            const messagesCabecera=[]
              console.log("----- creating 'viajes baja' messages-----")
              for(let i=number_seed; i<total_test+number_seed; i++){
                  const dataSend = viajeBaja;
                  dataSend.data.NRO_VIAJE= i+1//`${i+1}`;
                  dataSend.data.C_ESTADO= estado;
                  //dataSend.data.C_POSICION_ORDEN=1;
                  //dataSend.data.ancho_madre=`${5000+i}`;
                  
                  console.log("---------------------- linea ", i+1 , " -------------------------------------")
                  //console.log(a_mad)
                  //console.log(dataSend)
  
                  messagesCabecera.push({value: JSON.stringify(dataSend)});
                  messagesJSON.push(JSON.stringify(dataSend));
                  //messages.push({value: JSON.stringify(dataSend)});
              //    await  producer.send({
              //     topic: 'tpc-nca-viajes',
              //     messages: [
              //         { value: JSON.stringify(dataSend)  },
              //         { value: JSON.stringify(dataSend)  },
              //     ],
              //     })
              }
              console.log("----- finished 'viajes alta' messages-----")
              console.log("")
              console.log("")
              topicMessages.push({topic:"tpc-nca-viajes-baja", messages: messagesCabecera});
          }

            // console.log("----- finished viajes messages-----")
            // console.log("")
            // console.log("")
            // topicMessages.push({topic:"tpc-nca-viajes", messages: messages})

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
        //     fs.writeFile('./Data/viajes.json', jsonData, (error)=>{
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
            msg:'An error ocurred while sending the mesage to tpc-nca-viajes or tpc-nca-viajes-baja'
        })
    }

}


const SendMessageTpcNcaViajesBaja= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const brokers=[];
    brokers.push(host);
    const total_test=body.total_test;
    const data=body.data;
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

        const viaje={"data":{"NRO_VIAJE":46388500,"C_ESTADO":"BAV"},"domain":"APT","trx":"GeneracionViaje"}
        
       
        
        const topicMessages = [];

       
            const messages=[]
            console.log("----- creating viajes messages-----")
            for(let i=0; i< data.length; i++){
                const dataSend = viaje;
                dataSend.data.NRO_VIAJE= data[i];
                //dataSend.data.ancho_madre=`${5000+i}`;
                
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
            console.log("----- finished viajes messages-----")
            console.log("")
            console.log("")
            topicMessages.push({topic:"tpc-nca-viajes-baja", messages: messages})
        
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
    SendMessageTpcNcaViajes,
    SendMessageTpcNcaViajesBaja
}