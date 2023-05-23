job "rates-process" {
  datacenters = ["home"]
  type        = "batch"

  periodic {
    cron      = "20 19 * * * *"
    prohibit_overlap = true
  }

  group "rates-process-group" {
    count = 1
    task "rates-process-task" {
      driver = "docker"
      template {
        data = <<EOH

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
        image = "10.8.0.5:5000/rates-update:0.1.10"
        command = "bash"
        args = [
          "/app/run.sh",
          "0.1.10",
        ]
      }

      resources {
        cpu    = 1500
        memory = 3500
      }
    }
  }
}
