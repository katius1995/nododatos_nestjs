import { Inject, Injectable } from '@nestjs/common';
import { Producer } from 'kafkajs';
import { InjectConnection } from '@nestjs/typeorm';
import { Connection } from 'typeorm';

@Injectable()
export class AppService {
  constructor(
    @Inject('KafkaProducer')
    private readonly kafkaProducer: Producer,
    @InjectConnection()
    private readonly connection: Connection,
  ) {}

  async sendMessage(topic:string, data:any, key?:string) {
    return this.kafkaProducer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(data),
          key,
        },
      ],
    });
  }

  async searchWithParams(query:string, params:any, context:any, message:any) {
    console.log('QUERY = ' + query + ', PARAMS = ' + params);
    this.connection.query(query, params).then((value) => {
      console.log(value.length);
      const response = {};
      response['promise_valor'] = message.generales.promise_valor;
      response['contador'] = message.data.contador;
      if (value.length > 0) {
        console.log(value);
        response['estado_p'] = 200;
        response['data'] = value;
      } else {
        console.log('No hay datos');
        response['estado_p'] = 404;
        response['data'] = { mensaje: 'No hay datos.' };
      }
      this.sendMessage(context.getArgs()[0].key, response, 'nest_nododatos');
    });
  }

  async createData(query:string, params:any, context:any, message:any) {
    console.log('QUERY = ' + query + ', PARAMS = ' + params);
    const response = {};
    try {
      this.connection.query(query, params).then(
        (value) => {
          console.log(value);
          console.log(value.length);
          response['estado_p'] = 200;
          response['data'] = { mensaje: 'Ok.' };
          response['contador'] = message.data.contador;
          this.sendMessage(
            context.getArgs()[0].key,
            response,
            'nest_nododatos',
          );
        },
        (err) => {
          console.log(err);
          response['estado_p'] = 502;
          response['data'] = {
            mensaje: 'Error al ejecutar query. Razon: ' + err,
          };
          response['contador'] = message.data.contador;
          this.sendMessage(
            context.getArgs()[0].key,
            response,
            'nest_nododatos',
          );
        },
      );
    } catch (error) {
      response['estado_p'] = 502;
      response['data'] = {
        mensaje: 'Error al ejecutar query. Razon: ' + error,
      };
      response['contador'] = message.data.contador;
      this.sendMessage(context.getArgs()[0].key, response, 'nest_nododatos');
    }
  }
}
