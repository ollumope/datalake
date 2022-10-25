SHELL := /bin/bash

deploy:
	STEP=BUCKETS cdk deploy && \
	STEP=ROLES cdk deploy && \
	STEP=DB cdk deploy && \
	STEP=TABLE cdk deploy && \
	STEP=JOB cdk deploy && \
	STEP=ALL cdk deploy

destroy:
	cdk destroymake