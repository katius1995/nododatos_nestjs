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
  getNode(@Payload() message, @Ctx() context: KafkaContext) {
    console.log(context.getArgs()[0].key);
    console.log(context.getArgs()[0].value);
    //'SELECT * FROM usuarios WHERE codigo=$1 and psw=$2'
    if (message.generales.operacion === 'search')
      this.appService.searchWithParams(
        message.generales.sql,
        message.generales.params,
        context,
        message,
      );
    else if (
      message.generales.operacion === 'modify' ||
      message.generales.operacion === 'create' ||
      message.generales.operacion === 'delete'
    )
      this.appService.createData(
        message.generales.sql,
        message.generales.params,
        context,
        message,
      );
  }
}
