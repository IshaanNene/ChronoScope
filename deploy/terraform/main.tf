# Terraform for the cluster chronolog runs on.
#
# Deliberately provisions infrastructure only — the workload itself is the Helm
# chart. Managing pods through Terraform means every rollout is a state-file
# operation, and a consensus cluster wants rollouts to be boring and frequent.

terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.30" }
  }
}

variable "cluster_name" {
  type    = string
  default = "chronolog"
}

variable "replicas" {
  type    = number
  default = 3

  validation {
    # An even cluster tolerates no more failures than the odd one below it and
    # needs an extra acknowledgement per commit. Catch it at plan time.
    condition     = var.replicas % 2 == 1
    error_message = "replicas must be odd: consensus needs a majority."
  }
}

variable "storage_class" {
  type        = string
  default     = "fast-ssd"
  description = "fsync latency is the throughput ceiling; do not use network storage."
}

resource "kubernetes_namespace" "chronolog" {
  metadata {
    name   = var.cluster_name
    labels = { "app.kubernetes.io/name" = "chronolog" }
  }
}

# Losing a quorum simultaneously loses the cluster. This bounds voluntary
# disruption (drains, upgrades) to one node at a time; it does not bound
# involuntary disruption, which is what the simulator is for.
resource "kubernetes_pod_disruption_budget_v1" "chronolog" {
  metadata {
    name      = var.cluster_name
    namespace = kubernetes_namespace.chronolog.metadata[0].name
  }
  spec {
    max_unavailable = "1"
    selector { match_labels = { app = "chronolog" } }
  }
}

output "namespace" {
  value = kubernetes_namespace.chronolog.metadata[0].name
}
