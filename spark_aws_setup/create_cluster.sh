# Add your cluster name
aws emr create-cluster --name spark-cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--applications Name=Spark \
--ec2-attributes KeyName=spark-cluster-key \
--instance-type m5.xlarge \
--instance-count 3 \
# --auto-terminate
