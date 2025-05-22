stream:
	@npx node -r dotenv/config lib/publisher.js

consume:
	@npx node -r dotenv/config lib/consumer.js

serve:
	@npx squid-graphql-server


typegen:
	@npx squid-substrate-typegen typegen.json


up:
	@docker compose up -d


down:
	@docker compose down -v


.PHONY: stream consume serve start up down
