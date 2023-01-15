echo JDBC_URL=$(consul kv get jdbc.url)
echo JDBC_DRIVER=$(consul kv get jdbc.driver)
echo JDBC_USER=$(consul kv get jdbc.user)
echo JDBC_PASSWORD=$(consul kv get jdbc.password)

echo POSTGRES_METASTORE_JDBC_URL=$(consul kv get hive.postgres.metastore.jdbc.url)
echo POSTGRES_JDBC_URL=$(consul kv get postgres.jdbc.url)
echo POSTGRES_JDBC_DRIVER=$(consul kv get postgres.jdbc.driver)
echo POSTGRES_JDBC_USER=$(consul kv get postgres.jdbc.user)
echo POSTGRES_JDBC_PASSWORD=$(consul kv get postgres.jdbc.password)

echo S3_ENDPOINT=$(consul kv get expenses/object/storage/fs.s3a.endpoint)
echo S3_ACCESS_KEY=$(consul kv get expenses/object/storage/fs.s3a.access.key)
echo S3_SECRET_KEY=$(consul kv get expenses/object/storage/fs.s3a.secret.key)

echo SERVICE_MATCHER_BASE_URL=$(consul kv get expenses/service/matcher/base_url)
echo SERVICE_GOALS_BASE_URL=$(consul kv get telegram/bot/accounter/goals.base.url)
echo SERVICE_SPREADSHEETS_BASE_URL=$(consul kv get expenses/google/base_url)
