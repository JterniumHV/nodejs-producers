const express = require('express');
const { Kafka } = require('kafkajs')
const fs = require("fs")
const OFAS= require('../../models/PRUEBAS_MAGENTTA');
const PRUEBAS_MAGENTTA= require('../../models/PRUEBAS_MAGENTTA');


//------------------------------------------------------------------
//                      Send message tpc ofa
//------------------------------------------------------------------

const SendMessageTpcOFA= async (req, res = express.response) =>{

    const body = req.body;
    console.log(body)
    const host=body.host;
    const brokers=[];
    brokers.push(host);
    const total_test=       Number(body.total_test);
    const CABECERA=         body.cabecera;
    const PRIORIZACION=     body.priorizacion;
    const SKU=              body.sku;
    const AVANCE=           body.avance;
    const VL=               body.vl;
    const name_test=        body.name_test;
    const number_seed=      Number(body.number_seed);
    const type=             body.type;
    const action =          body.action;
    const time =            Number(body.time);

    //return;

    console.log(`Cabecera: ${CABECERA}, Priorizacion: ${PRIORIZACION}, Sku: ${SKU}, Avance: ${AVANCE}`)
    //const data =body.data;

    
    
    const kafka = new Kafka({brokers:brokers})
    console.log(kafka)
    console.log("creando producer")
        const producer = kafka.producer({
                allowAutoTopicCreation: false,
                transactionTimeout: 300000,
                //createPartitioner: Partitioners.DefaultPartitioner
            })
    await producer.connect()
    console.log(producer)
    try {
        

        const ofa={
            "data": {
                "c_id_ingenieria": 7733058,
                "c_necesidad_id": "003003874344",
                "v_vestido": 8492275,
                "f_prioridad": "2022-11-08T00:00:00.000Z",
                "d_cliente": "TERNIUM MEXICO, S.A. DE C.V.",
                "c_tipo_estado": "ACEPTADA",
                "c_cliente": "N000000885",
                "UTCTime": 1664329620,
                "c_ofa_id": "0503748519000010",
                "um_estado": "2022-09-27T20:41:59.000Z",
                "q_cantidad": 30.9,
                "PaisOFA": "MX",
                "r_vestido": 8492275,
                "c_busqueda_ruta": "A1_CSCA",
                "timestamp": "2022-09-27T20:47:00.000Z",
                "f_promesa": "2022-11-08T00:00:00.000Z",
                "c_clave": "MTSCOMEREDC",
                "q_cantidad_a_ingresar": 30.9,
                "f_repromesa": "2022-11-08T00:00:00.000Z",
                "c_clase_prodto": "M-C-CINCALUM-TRA-_",
                "um_necesidad": "2022-09-26T18:49:07.000Z",
                "c_ruta": 90700777,
                "c_norma": "ASTM A 792",
                "c_prodto": "MC500208",
                "c_id_ruta_ing": 81944043,
                "tol_neg": 15,
                "ofa_tipo": "CLIENTE",
                "tol_pos": 5,
                "est_ofa_c_tipo_estado": "ACEPTADA"
            },
            "domain": "OFA",
            "trx": "OrdenesFabricacion"
        }

        const priorizacionOFA={
            "data": {
                "PriorizacionOFA": {
                    "Pedido": "0300815999",
                    "Workflow": "0037653169",
                    "PaisOFA": "MX",
                    "OFA": "0300815999000010",
                    "Tipos": [
                        {
                            "Tipo_demanda": "PEDIDO",
                            "Priorizacion": "URGENTE",
                            "Fecha_critico": "2022-05-23T00:00:00.000Z",
                            "cantidad_udc": "60.0",
                            "Id_udc": "4577456",
                            "Estado": "ACTIVO"
                        }
                    ],
                    "Posicion": "000010"
                }
            },
            "domain": "OFA",
            "trx": "PriorizacionOFA"
        }

        const ofaIdStock={
            
            "data" : {
              "clase_desc" : "PERFIL PREPINTADO TABLERO",
              "uso_general":"GALV. AUTOMOTRIZ",
              "c_necesidad_id" : "003003844875",
              "uso_final" : "STD",
              "ancho_madre" : "1220",
              "Piezas_Por_PAQ" : "233",
              "c_TIPO_OFA" : "CATALOGO",
              "clase_cod" : "M-R-PREPINTADO-TAB-_",
              "c_ID_RED" : "26704_PT",
              "peso_max" : 1,
              "UTCTime" : 1664911220,
              "embalaje" : "EU12",
              "PaisOFA" : "MX",
              "grupo_desc" : "PREPINTADO",
              "peso_min" : 0,
              "C_ID_STOCK" : 1761500,
              "c_SKU" : "ID_1761500",
              "timestamp" : "2022-10-04T14:20:20.003Z"
            },
            "domain" : "OFA",
            "trx" : "SkuOFA"
          
    }

    const ofaVariables={
        "data" : {
          "OFAKeys" : {
            "c_necesidad_id" : "002001489484",
            "n_id_ejecucion" : "51037718",
            "_nca_log_id" : "2023-01-13T10:31:59.000Z",
            "PaisOFA" : "MX",
            "c_ruta" : "94891237",
            "c_llave" : "85102133",
            "Lineas" : [ {
              "n_secuencia" : "1",
              "VariablesLinea" : [ {
                "m_valor_indicador" : "V",
                "dominio" : "LINEA",
                "c_id" : "29407618616",
                "c_formato" : "N ",
                "n_valor_desde" : "1220",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ANCHO_S"
              }, {
                "t_valor_desde" : "TRA",
                "m_valor_indicador" : "R",
                "d_descripcion_valor" : "TRA",
                "t_valor_hasta" : "TRA",
                "dominio" : "LINEA",
                "c_id" : "29407618622",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "FORMA_S"
              }, {
                "t_valor_desde" : "TR-101",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "AUTO",
                "dominio" : "LINEA",
                "c_id" : "29407618636",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PERFIL_RF"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618632",
                "c_formato" : "N ",
                "n_valor_hasta" : "1.0076",
                "n_valor_desde" : "1.0076",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PAMT"
              }, {
                "t_valor_desde" : "1",
                "m_valor_indicador" : "G",
                "t_valor_hasta" : "1",
                "dominio" : "LINEA",
                "c_id" : "29407618641",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PESO_INTERMEDIO"
              }, {
                "m_valor_indicador" : "R",
                "d_descripcion_valor" : "17.5",
                "dominio" : "LINEA",
                "c_id" : "29407618619",
                "c_formato" : "N ",
                "n_valor_hasta" : "17.5",
                "n_valor_desde" : "17.5",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PESO_MAX_E"
              }, {
                "m_valor_indicador" : "V",
                "dominio" : "LINEA",
                "c_id" : "29407618620",
                "c_formato" : "N ",
                "n_valor_desde" : "10230",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "LARGO_S"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618627",
                "c_formato" : "N ",
                "n_valor_hasta" : "2.5",
                "n_valor_desde" : ".5",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PESO_LIMITE_S"
              }, {
                "t_valor_desde" : "ESTANDAR",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "AUTO",
                "dominio" : "LINEA",
                "c_id" : "29407618639",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ALIM_ROLLO"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618634",
                "c_formato" : "N ",
                "n_valor_hasta" : "1.0116",
                "n_valor_desde" : "1.0116",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PAMT_TOTAL"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618621",
                "c_formato" : "N ",
                "n_valor_hasta" : "5",
                "n_valor_desde" : "0",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "TOL_ANCHO"
              }, {
                "t_valor_desde" : "PINTTRAP_CUB_COM-COM_ESTRUCTURAL",
                "m_valor_indicador" : "R",
                "d_descripcion_valor" : "PINTTRAP_CUB",
                "t_valor_hasta" : "PINTTRAP_CUB_COM-COM_ESTRUCTURAL",
                "dominio" : "LINEA",
                "c_id" : "29407618626",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "AGRUP_SITE_CALI"
              }, {
                "t_valor_desde" : "ACABADO / FONDO (AC+PR|PR+AC)",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "AUTO",
                "dominio" : "LINEA",
                "c_id" : "29407618640",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ESQ_PINTADO_RF"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618623",
                "c_formato" : "N ",
                "n_valor_hasta" : ".0584",
                "n_valor_desde" : ".0584",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "TOL_ESP"
              }, {
                "m_valor_indicador" : "V",
                "dominio" : "LINEA",
                "c_id" : "29407618624",
                "c_formato" : "N ",
                "n_valor_desde" : ".5437",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ESPESOR_E"
              }, {
                "m_valor_indicador" : "V",
                "dominio" : "LINEA",
                "c_id" : "29407618629",
                "c_formato" : "N ",
                "n_valor_desde" : ".5437",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ESPESOR_S"
              }, {
                "t_valor_desde" : "EA15",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "EA15",
                "dominio" : "LINEA",
                "c_id" : "29407618635",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ETE"
              }, {
                "t_valor_desde" : "VIDEOJET",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "AUTO",
                "dominio" : "LINEA",
                "c_id" : "29407618638",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "TIPO_SELLO_RF"
              }, {
                "m_valor_indicador" : "V",
                "dominio" : "LINEA",
                "c_id" : "29407618617",
                "c_formato" : "N ",
                "n_valor_desde" : "1220",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "ANCHO_E"
              }, {
                "t_valor_desde" : "INTERNA",
                "m_valor_indicador" : "P",
                "d_descripcion_valor" : "AUTO",
                "dominio" : "LINEA",
                "c_id" : "29407618637",
                "c_formato" : "S ",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "SUPERFICIE_RF"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618630",
                "c_formato" : "N ",
                "n_valor_hasta" : "5.5383",
                "n_valor_desde" : "5.5383",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PROD_NETA"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618633",
                "c_formato" : "N ",
                "n_valor_hasta" : "1.004",
                "n_valor_desde" : "1.004",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PAMC"
              }, {
                "m_valor_indicador" : "R",
                "d_descripcion_valor" : "3.5",
                "dominio" : "LINEA",
                "c_id" : "29407618615",
                "c_formato" : "N ",
                "n_valor_hasta" : "3.5",
                "n_valor_desde" : "3.5",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PESO_MAX_S"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618618",
                "c_formato" : "N ",
                "n_valor_hasta" : "2.519",
                "n_valor_desde" : ".504",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PESO_LIMITE_E"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618625",
                "c_formato" : "N ",
                "n_valor_hasta" : "5",
                "n_valor_desde" : "0",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "TOL_LARGO"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618628",
                "c_formato" : "N ",
                "n_valor_hasta" : "53064",
                "n_valor_desde" : "53064",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "KPZ"
              }, {
                "m_valor_indicador" : "R",
                "dominio" : "LINEA",
                "c_id" : "29407618631",
                "c_formato" : "N ",
                "n_valor_hasta" : "5.2701",
                "n_valor_desde" : "5.2701",
                "c_linea_var" : "437953581",
                "c_codigo_variable" : "PROD_DISP"
              } ],
              "c_linea_prodtva" : "A1_CSCA"
            } ],
            "_dwh_log_id" : "11631085712371"
          }
        },
        "domain" : "OFA",
        "trx" : "VariablesLinea"
      }

      const ofaAvance = {
        "data" : {
          "AvanceOFA" : {
            "c_necesidad_id" : "003004017571",
            "c_ofa_id" : "0503857814000010",
            "PaisOFA" : "MX",
            "Lineas" : [ {
              "Saldos" : [ {
                "ton_stock_en_proceso_entrada" : "0.0",
                "ton_stock_material_salida_x_linea" : "0.0",
                "ton_stock_material_lanzado_x_linea" : "0.0",
                "ton_stock_materiales_proy" : "0.0",
                "ton_stock_suspendido_salida" : "0.0",
                "ton_stock_material_a_lanzar_x_linea" : "6.14",
                "ton_stock_en_proceso_proy_x_linea" : "0.0",
                "ton_stock_materiales_x_linea" : "0.0",
                "fechaCalculoAvance" : "2023-01-13T15:25:17.000Z",
                "ton_stock_material_salida_proy_x_linea" : "0.0",
                "ton_stock_otros_x_linea" : "0.0",
                "ton_stock_material_entrada_x_linea" : "0.0",
                "sec" : "2.0",
                "ton_stock_apropiadas_x_linea" : "0.0",
                "ton_stock_material_entrada_proy_x_linea" : "0.0",
                "ton_stock_apropiadas_salida" : "0.0",
                "tons_stk_ingresado" : "0.0",
                "ton_stock_excedentes_x_linea" : "0.0",
                "ton_stock_en_proceso_x_linea" : "0.0",
                "ton_stock_suspendido_entrada" : "0.0",
                "ton_saldo_a_lanzar_x_linea" : "6.14",
                "ton_stock_otros_proy_x_linea" : "0.0",
                "ton_stock_programado_x_linea" : "0.0",
                "ton_stock_apropiadas_proy_x_linea" : "0.0",
                "ton_stock_otros_salida" : "0.0",
                "ton_stock_cantidad_programada_x_linea" : "0.0",
                "saldo_a_ingresar" : "6.14",
                "ton_stock_en_proceso_salida" : "0.0",
                "saldo_a_programar" : "6.238",
                "ton_stock_disponible_salida" : "0.0",
                "ton_stock_materiales_ciclo_proy" : "0.0",
                "ton_saldo_a_programar_x_linea" : "6.14",
                "ton_stock_disponible_x_linea" : "0.0",
                "ton_stock_programado_proy_x_linea" : "0.0",
                "ton_stock_suspendido_x_linea" : "0.0",
                "ton_stock_disponible_entrada" : "0.0",
                "ton_stock_suspendido_proy_x_linea" : "0.0",
                "ton_stock_otros_entrada" : "0.0",
                "saldo_a_procesar" : "6.14",
                "ton_stock_disponible_proy_x_linea" : "0.0",
                "ton_stock_apropiadas_entrada" : "0.0"
              } ],
              "linea_esta_en_ruta" : "SI",
              "secuencia_linea" : "2.0",
              "cod_linea" : "EMBY6_CSIS"
            }, {
              "Saldos" : [ {
                "ton_stock_en_proceso_entrada" : "0.0",
                "ton_stock_material_salida_x_linea" : "0.0",
                "ton_stock_material_lanzado_x_linea" : "0.0",
                "ton_stock_materiales_proy" : "0.0",
                "ton_stock_suspendido_salida" : "0.0",
                "ton_stock_material_a_lanzar_x_linea" : "6.238",
                "ton_stock_en_proceso_proy_x_linea" : "0.0",
                "ton_stock_materiales_x_linea" : "0.0",
                "fechaCalculoAvance" : "2023-01-13T15:25:17.000Z",
                "ton_stock_material_salida_proy_x_linea" : "0.0",
                "ton_stock_otros_x_linea" : "0.0",
                "ton_stock_material_entrada_x_linea" : "0.0",
                "sec" : "1.0",
                "ton_stock_apropiadas_x_linea" : "0.0",
                "ton_stock_material_entrada_proy_x_linea" : "0.0",
                "ton_stock_apropiadas_salida" : "0.0",
                "tons_stk_ingresado" : "0.0",
                "ton_stock_excedentes_x_linea" : "0.0",
                "ton_stock_en_proceso_x_linea" : "0.0",
                "ton_stock_suspendido_entrada" : "0.0",
                "ton_saldo_a_lanzar_x_linea" : "6.238",
                "ton_stock_otros_proy_x_linea" : "0.0",
                "ton_stock_programado_x_linea" : "0.0",
                "ton_stock_apropiadas_proy_x_linea" : "0.0",
                "ton_stock_otros_salida" : "0.0",
                "ton_stock_cantidad_programada_x_linea" : "0.0",
                "saldo_a_ingresar" : "6.14",
                "ton_stock_en_proceso_salida" : "0.0",
                "saldo_a_programar" : "6.238",
                "ton_stock_disponible_salida" : "0.0",
                "ton_stock_materiales_ciclo_proy" : "0.0",
                "ton_saldo_a_programar_x_linea" : "6.238",
                "ton_stock_disponible_x_linea" : "0.0",
                "ton_stock_programado_proy_x_linea" : "0.0",
                "ton_stock_suspendido_x_linea" : "0.0",
                "ton_stock_disponible_entrada" : "0.0",
                "ton_stock_suspendido_proy_x_linea" : "0.0",
                "ton_stock_otros_entrada" : "0.0",
                "saldo_a_procesar" : "6.238",
                "ton_stock_disponible_proy_x_linea" : "0.0",
                "ton_stock_apropiadas_entrada" : "0.0"
              } ],
              "linea_esta_en_ruta" : "SI",
              "secuencia_linea" : "1.0",
              "cod_linea" : "SLY6_CSIS"
            } ]
          }
        },
        "domain" : "OFA",
        "trx" : "Avance"
      }

        let estado= "ACEPTADA"
        if(action=="baja"){
            estado= "CERRADA"
        }
        
       
        
        const topicMessages = [];

       
            //const messages=[]
            const messagesJSON=[]
            
            console.log(`Interval: ${number_seed} to: ${total_test+number_seed}`)
            //console.log(total_test+number_seed)
            //return
            //------------------------------------------------------------------------------------------
            //                               Algoritmo de Ofa Cabecera
            //------------------------------------------------------------------------------------------
            if(CABECERA){
                const messagesCabecera=[]
                console.log("----- creating ofa messages-----")
                for(let i=number_seed; i<total_test+number_seed; i++){
                    const dataSend = ofa;
                    dataSend.data.c_necesidad_id=`${i+1}`;
                    dataSend.data.c_tipo_estado= estado;
                    dataSend.data.c_ofa_id=`${Number(i+1)}`;
                    dataSend.data.ofa_tipo="CLIENTE";
                    dataSend.data.est_ofa_c_tipo_estado= estado;
                    //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                    //console.log(dataSend)
                    messagesCabecera.push({value: JSON.stringify(dataSend)});
                    messagesJSON.push(JSON.stringify(dataSend));
                }
                console.log("----- finished ofas messages-----")
                console.log("")
                console.log("")
                
                topicMessages.push({topic:"tpc-nca-ofa", messages: messagesCabecera})
            }

            //------------------------------------------------------------------------------------------
            //                               Algoritmo de Priorizacion OFA
            //------------------------------------------------------------------------------------------
            if(PRIORIZACION===true && action !== "baja"){
                const messagesPriorizacion=[]
                console.log("----- creating priorizacion messages-----")
                for(let i=number_seed; i<total_test+number_seed; i++){
                    const dataSend = priorizacionOFA;
                    dataSend.data.PriorizacionOFA.OFA=`${i+1}`;
                    //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                    //console.log(dataSend)
                    messagesPriorizacion.push({value: JSON.stringify(dataSend)});
                    messagesJSON.push(JSON.stringify(dataSend));
                }
                console.log("----- finished priorizacion messages-----")
                console.log("")
                console.log("")
                
                topicMessages.push({topic:"tpc-nca-ofa", messages: messagesPriorizacion})
            }

            //------------------------------------------------------------------------------------------
            //                               Algoritmo de SKU Avance
            //------------------------------------------------------------------------------------------
            if(SKU===true && action !== "baja"){
                const messagesSKU=[]
                console.log("----- creating sku id stock messages-----")
                for(let i=number_seed; i<total_test+number_seed; i++){
                    const dataSend = ofaIdStock;
                    dataSend.data.c_necesidad_id=`${i+1}`;
                    dataSend.data.uso_general=`GALV. AUTOMOTRIZ orden ${i+1}`;
                    //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                    //console.log(dataSend)
                    messagesSKU.push({value: JSON.stringify(dataSend)});
                    messagesJSON.push(JSON.stringify(dataSend));
                }
                console.log("----- finished sku id stock messages-----")
                console.log("")
                console.log("")
                
                topicMessages.push({topic:"tpc-ofa-id-stock", messages: messagesSKU})
            }

            //------------------------------------------------------------------------------------------
            //                               Algoritmo de Variables
            //------------------------------------------------------------------------------------------
            if(VL===true && action !== "baja"){
                const messagesVL=[]
                console.log("----- creating ofa variables messages-----")
                for(let i=number_seed; i<total_test+number_seed; i++){
                    const dataSend = ofaVariables;
                    dataSend.data.OFAKeys.c_necesidad_id=`${i+1}`;
                    //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                    //console.log(dataSend)
                    messagesVL.push({value: JSON.stringify(dataSend)});
                    messagesJSON.push(JSON.stringify(dataSend));
                }
                console.log("----- finished OFA VARIABLES messages-----")
                console.log("")
                console.log("")
                
                topicMessages.push({topic:"tpc-nca-ofa-vl", messages: messagesVL})
            }

            //------------------------------------------------------------------------------------------
            //                               Algoritmo de Avance
            //------------------------------------------------------------------------------------------
            if(AVANCE===true && action !== "baja"){
                const messagesAvance=[]
                console.log("----- creating ofa avance messages-----")
                for(let i=number_seed; i<total_test+number_seed; i++){
                    const dataSend = ofaAvance;
                    dataSend.data.AvanceOFA.c_necesidad_id=`${i+1}`;
                    dataSend.data.AvanceOFA.c_ofa_id=`${i+1}`;
                    //console.log("---------------------- linea ", i+1 , " -------------------------------------")
                    //console.log(dataSend)
                    messagesAvance.push({value: JSON.stringify(dataSend)});
                    messagesJSON.push(JSON.stringify(dataSend));
                }
                console.log("----- finished OFA AVANCE messages-----")
                console.log("")
                console.log("")
                
                topicMessages.push({topic:"tpc-nca-ofa-avance", messages: messagesAvance})
            }


        
            const initial= new Date()
            console.log("----- sending messages-----")
            console.log("Init time: ", initial)
           
            console.log("Enviando bloque de datos")
            await producer.sendBatch({
                topicMessages,
                //acks: 0,
                timeout: 300000,
            })

            console.log("desconectando producer...")
            await producer.disconnect()
            const end= new Date()
            console.log("End time: ", end)
            console.log("producer desconectado")
            console.log("")
            console.log("")

            const initTimeOut= new Date()
            
            setTimeout(async()=>{
            const endTimeOut= new Date()
            console.log(`Querying OFAS from: ${number_seed+1} to: ${number_seed+total_test} ...`)
            const ofas = await OFAS.aggregate([
                {$match: 
                    {
                        $and:[
                            { $expr: { $gt: [ { $toInt: "$_id" }, number_seed+1 ] }},
                            //{"_id": { $gt : number_seed }},
                            { $expr: { $lte: [ { $toInt: "$_id" }, number_seed+total_test ] }},
                            //{"_id": { $lte : number_seed+total_test }}
                        ]
                    }
                },
                {$sort:{ 
                    timestamp : 1
                    }

                },
                {$project:{
                    "timestamp": "$timestamp"
                    }
                }
                
            ]);
                
                console.log(ofas)
                console.log(`Timeout inicial: ${initTimeOut.toISOString()}, Timeout final: ${endTimeOut.toISOString()}, OFAS count: ${ofas.length}`)
                console.log("timestamp 1 "+ofas[0].timestamp)
                console.log("timestamp 2 "+ ofas[ofas.length-1].timestamp)
                const initTimeWrite= new Date (Number(ofas[0].timestamp))
                const endTimeWrite= new Date (Number(ofas[ofas.length-1].timestamp))

                console.log(`Tiempo de inicio: ${initTimeWrite}, Tiempo final: ${endTimeWrite}`)
                console.log(`Ejecucion en: ${(endTimeWrite.getTime()-initTimeWrite.getTime())/(1000)} - Segundos`)
                const Operation_Time = Number((endTimeWrite.getTime()-initTimeWrite.getTime())/(1000))
                const Transfer_Rate = (ofas.length/Operation_Time)
                const Process_Data = ofas.length
                const Status = (Number(ofas.length) === Number(total_test))? true : false;

                console.log(`Tiempo de operacion: ${Operation_Time}, Tasa de transferencia: ${Transfer_Rate}, Datos Procesados: ${Process_Data}, Status: ${Status}`)

                const continueTimeOut= new Date()

            console.log(`Timeout continue: ${continueTimeOut}`)


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
            // let jsonData = JSON.stringify(messagesJSON);
            // //console.log(jsonData)
            // fs.writeFile('./Data/ofas.json', jsonData, (error)=>{
            //     if(error){
            //         console.log(`Error: ${error}`);
            //     }else{
            //         console.log(`Archivo guardado correctamente`);
            //     }
            // });
            
                res.status(200).json({
                    ok:true,
                    msg:'Message OK',
                    data: topicMessages
                })

            }, time*1000);
            
            
        
        
        
    } catch (error) {
        console.log(error);
        return res.status(200).json({
            ok:false,
            msg:'An error ocurred while sending the mesage to tpc-nca-ofa'
        })
    }

}

module.exports={
    SendMessageTpcOFA
}