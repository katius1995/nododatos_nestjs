import { Controller } from '@nestjs/common';
import {
  Ctx,
  KafkaContext,
  MessagePattern,
  Payload,
} from '@nestjs/microservices';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @MessagePattern('nest_nododatos') // Our topic name
  getHello(@Payload() message, @Ctx() context: KafkaContext) {
    console.log(context.getArgs()[0].key);
    console.log(context.getArgs()[0].value);
    //'SELECT * FROM usuarios WHERE codigo=$1 and psw=$2'
    if (message.generales.operacion === 'consultar')
      this.appService.consultarConParams(
        message.generales.sql,
        message.generales.params,
        context,
        message,
      );
    else if (
      message.generales.operacion === 'modificar' ||
      message.generales.operacion === 'crear' ||
      message.generales.operacion === 'eliminar'
    )
      this.appService.crearDato(
        message.generales.sql,
        message.generales.params,
        context,
        message,
      );
  }
}
