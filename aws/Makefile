build_sec:
	sam deploy -t template_snowpipe_secret.yaml --guided

build:
	sam build --use-container

deploy:
	sam deploy --capabilities CAPABILITY_NAMED_IAM --guided

delete:
	sam delete
