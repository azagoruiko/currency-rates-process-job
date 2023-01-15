job "rates-update" {
  datacenters = ["home"]
  type        = "batch"

  periodic {
    cron      = "10 20 * * * *"
    prohibit_overlap = true
  }

  group "rates-update-group" {
    count = 1
    task "rates-update-task" {
      driver = "docker"
      template {
        data = <<EOH
POSTGRES_JDBC_URL="{{ key "postgres.jdbc.url" }}"
POSTGRES_JDBC_DRIVER="{{ key "postgres.jdbc.driver" }}"
POSTGRES_JDBC_USER="{{ key "postgres.jdbc.user" }}"
POSTGRES_JDBC_PASSWORD="{{ key "postgres.jdbc.password" }}"
EOH
        destination = "secrets.env"
        env = true
      }

      config {
        network_mode = "host"
        extra_hosts = ["nuc2:10.8.0.8", "nuc3:10.8.0.6", "nuc1:10.8.0.9", "vm1:10.8.0.2"]
        privileged = true
        image = "127.0.0.1:9999/docker/rates-update:0.0.93"
        command = "bash"
        args = [
          "/app/run.sh",
          "0.0.93",
        ]
      }

      resources {
        cpu    = 1500
        memory = 1500
      }
    }
  }
}
