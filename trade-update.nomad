job "trade-update" {
  datacenters = ["home"]
  type        = "batch"

  periodic {
    cron      = "20 19 * * * *"
    prohibit_overlap = true
  }

  group "trade-update-group" {
    count = 1
    task "trade-update-task" {
      driver = "docker"
      template {
        data = <<EOH
SPARK_LOCAL_IP="{{ env "attr.unique.network.ip-address" }}"
SPARK_LOCAL_HOSTNAME="{{ env "attr.unique.network.ip-address" }}"
POSTGRES_JDBC_URL="{{ key "postgres.jdbc.url" }}"
POSTGRES_JDBC_DRIVER="{{ key "postgres.jdbc.driver" }}"
POSTGRES_JDBC_USER="{{ key "postgres.jdbc.user" }}"
POSTGRES_JDBC_PASSWORD="{{ key "postgres.jdbc.password" }}"

JDBC_URL="{{ key "jdbc.url" }}"
JDBC_DRIVER="{{ key "jdbc.driver" }}"
JDBC_USER="{{ key "jdbc.user" }}"
JDBC_PASSWORD="{{ key "jdbc.password" }}"

POSTGRES_METASTORE_JDBC_URL="{{ key "hive.postgres.metastore.jdbc.url" }}"
POSTGRES_JDBC_URL="{{ key "postgres.jdbc.url" }}"
POSTGRES_JDBC_DRIVER="{{ key "postgres.jdbc.driver" }}"
POSTGRES_JDBC_USER="{{ key "postgres.jdbc.user" }}"
POSTGRES_JDBC_PASSWORD="{{ key "postgres.jdbc.password" }}"

S3_ENDPOINT="{{ key "expenses/object/storage/fs.s3a.endpoint" }}"
S3_ACCESS_KEY="{{ key "expenses/object/storage/fs.s3a.access.key" }}"
S3_SECRET_KEY="{{ key "expenses/object/storage/fs.s3a.secret.key" }}"
S3_SHARED_BUCKET="{{ key "expenses/object/storage/shared_bucket" }}"

SERVICE_MATCHER_BASE_URL="{{ key "expenses/service/matcher/base_url" }}"
SERVICE_GOALS_BASE_URL="{{ key "telegram/bot/accounter/goals.base.url" }}"
SERVICE_SPREADSHEETS_BASE_URL="{{ key "expenses/google/base_url" }}"

{{ range service "spark-master" }}
SPARK_MASTER={{ .Address }}:7077

{{ end }}
EOH
        destination = "secrets.env"
        env = true
      }

      config {
        network_mode = "host"
        privileged = true
        image = "10.8.0.5:5000/rates-update:0.2.1"
        command = "bash"
        args = [
          "/app/run.sh"
        ]
      }

      resources {
        cpu    = 1500
        memory = 2500
      }
    }
  }
}
