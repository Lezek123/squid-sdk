import {RabbitMQDatabase} from '@subsquid/rabbitmq-store'
import { processor } from './processor'

processor.stream(new RabbitMQDatabase())
