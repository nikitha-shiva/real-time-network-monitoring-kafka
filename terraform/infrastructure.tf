# Real-Time Network Monitoring Infrastructure
# Terraform configuration for Kafka + Spark + Azure Data Explorer

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Local variables
locals {
  project_name = "network-monitoring"
  environment  = var.environment
  location     = var.location
  
  common_tags = {
    Project     = "Real-Time Network Monitoring"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = "DataEngineering"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "${local.project_name}-${local.environment}-rg"
  location = local.location
  tags     = local.common_tags
}

# Azure Kubernetes Service (AKS) for Kafka and Spark
resource "azurerm_kubernetes_cluster" "main" {
  name                = "${local.project_name}-${local.environment}-aks"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = "${local.project_name}-${local.environment}"

  default_node_pool {
    name                = "default"
    node_count          = var.aks_node_count
    vm_size            = var.aks_vm_size
    type               = "VirtualMachineScaleSets"
    availability_zones = ["1", "2", "3"]
    
    # Auto-scaling configuration
    enable_auto_scaling = true
    min_count          = var.aks_min_nodes
    max_count          = var.aks_max_nodes
    
    # Node pool configuration
    max_pods        = 110
    os_disk_size_gb = 100
    os_disk_type    = "Managed"
  }

  # Additional node pool for Spark workers
  depends_on = [azurerm_kubernetes_cluster.main]
}

resource "azurerm_kubernetes_cluster_node_pool" "spark_workers" {
  name                  = "sparkworkers"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size              = var.spark_worker_vm_size
  
  # Auto-scaling for dynamic workloads
  enable_auto_scaling = true
  node_count         = var.spark_worker_node_count
  min_count          = var.spark_worker_min_nodes
  max_count          = var.spark_worker_max_nodes
  
  # Spot instances for cost optimization
  priority        = "Spot"
  eviction_policy = "Delete"
  spot_max_price  = 0.5  # Max price per hour
  
  # Node taints for Spark workloads
  node_taints = ["spark-workload=true:NoSchedule"]
  
  tags = local.common_tags
}

# Service Principal for AKS
resource "azurerm_kubernetes_cluster" "main" {
  # ... previous configuration ...

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin    = "azure"
    network_policy    = "azure"
    dns_service_ip   = "10.2.0.10"
    service_cidr     = "10.2.0.0/24"
    docker_bridge_cidr = "172.17.0.1/16"
  }

  tags = local.common_tags
}

# Azure Data Explorer (Kusto) Cluster
resource "azurerm_kusto_cluster" "main" {
  name                = "${local.project_name}${local.environment}kusto"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  sku {
    name     = var.kusto_sku_name
    capacity = var.kusto_sku_capacity
  }

  # Enable auto-scaling
  optimized_auto_scale {
    minimum_instances = var.kusto_min_instances
    maximum_instances = var.kusto_max_instances
  }

  tags = local.common_tags
}

# Kusto Database for Network Logs
resource "azurerm_kusto_database" "network_logs" {
  name                = "NetworkLogs"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  cluster_name        = azurerm_kusto_cluster.main.name

  hot_cache_period   = "P7D"   # 7 days hot cache
  soft_delete_period = "P365D" # 1 year retention
}

# Storage Account for checkpoints and logs
resource "azurerm_storage_account" "main" {
  name                     = "${local.project_name}${local.environment}storage"
  resource_group_name      = azurerm_resource_group.main.name
  location                = azurerm_resource_group.main.location
  account_tier            = "Standard"
  account_replication_type = "LRS"
  
  # Enable hierarchical namespace for Data Lake Gen2
  is_hns_enabled = true

  # Network rules for security
  network_rules {
    default_action             = "Deny"
    virtual_network_subnet_ids = [azurerm_subnet.aks_subnet.id]
    ip_rules                   = var.allowed_ip_addresses
  }

  tags = local.common_tags
}

# Storage containers
resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "logs" {
  name                  = "logs"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Virtual Network for AKS
resource "azurerm_virtual_network" "main" {
  name                = "${local.project_name}-${local.environment}-vnet"
  address_space       = ["10.1.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

# Subnet for AKS nodes
resource "azurerm_subnet" "aks_subnet" {
  name                 = "aks-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.1.1.0/24"]
}

# Network Security Group
resource "azurerm_network_security_group" "aks_nsg" {
  name                = "${local.project_name}-${local.environment}-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  # Allow Kafka traffic
  security_rule {
    name                       = "AllowKafka"
    priority                   = 1001
    direction                 = "Inbound"
    access                    = "Allow"
    protocol                  = "Tcp"
    source_port_range         = "*"
    destination_port_range    = "9092"
    source_address_prefix     = "10.1.0.0/16"
    destination_address_prefix = "*"
  }

  # Allow Spark UI
  security_rule {
    name                       = "AllowSparkUI"
    priority                   = 1002
    direction                 = "Inbound"
    access                    = "Allow"
    protocol                  = "Tcp"
    source_port_range         = "*"
    destination_port_range    = "4040"
    source_address_prefix     = "10.1.0.0/16"
    destination_address_prefix = "*"
  }

  tags = local.common_tags
}

# Associate NSG with subnet
resource "azurerm_subnet_network_security_group_association" "aks_nsg_association" {
  subnet_id                 = azurerm_subnet.aks_subnet.id
  network_security_group_id = azurerm_network_security_group.aks_nsg.id
}

# Log Analytics Workspace for monitoring
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${local.project_name}-${local.environment}-logs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.common_tags
}

# Application Insights
resource "azurerm_application_insights" "main" {
  name                = "${local.project_name}-${local.environment}-insights"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"

  tags = local.common_tags
}

# Key Vault for secrets
resource "azurerm_key_vault" "main" {
  name                = "${local.project_name}-${local.environment}-kv"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name           = "standard"

  # Network access rules
  network_acls {
    default_action             = "Deny"
    virtual_network_subnet_ids = [azurerm_subnet.aks_subnet.id]
    ip_rules                   = var.allowed_ip_addresses
  }

  tags = local.common_tags
}

# Data source for current client config
data "azurerm_client_config" "current" {}

# Key Vault access policy for current user
resource "azurerm_key_vault_access_policy" "current_user" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get", "List", "Set", "Delete", "Purge", "Recover"
  ]
}

# Key Vault access policy for AKS managed identity
resource "azurerm_key_vault_access_policy" "aks_identity" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = azurerm_kubernetes_cluster.main.identity[0].tenant_id
  object_id    = azurerm_kubernetes_cluster.main.identity[0].principal_id

  secret_permissions = [
    "Get", "List"
  ]
}

# Helm provider configuration
provider "helm" {
  kubernetes {
    host                   = azurerm_kubernetes_cluster.main.kube_config.0.host
    client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)
    client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_key)
    cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)
  }
}

# Kubernetes provider configuration
provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.main.kube_config.0.host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)
}

# Kafka deployment using Helm
resource "helm_release" "kafka" {
  name             = "kafka"
  repository       = "https://charts.bitnami.com/bitnami"
  chart           = "kafka"
  namespace       = "kafka"
  create_namespace = true

  values = [
    yamlencode({
      replicaCount = var.kafka_replicas
      
      # Resource allocation
      resources = {
        limits = {
          memory = "2Gi"
          cpu    = "1000m"
        }
        requests = {
          memory = "1Gi"
          cpu    = "500m"
        }
      }
      
      # Persistence
      persistence = {
        enabled      = true
        storageClass = "managed-premium"
        size         = "100Gi"
      }
      
      # Auto-scaling
      autoscaling = {
        enabled     = true
        minReplicas = var.kafka_min_replicas
        maxReplicas = var.kafka_max_replicas
        targetCPU   = 70
      }
      
      # JVM settings for performance
      heapOpts = "-Xms1G -Xmx1G"
      
      # Kafka configuration
      config = {
        "num.network.threads"               = "8"
        "num.io.threads"                   = "8"
        "socket.send.buffer.bytes"         = "102400"
        "socket.receive.buffer.bytes"      = "102400"
        "socket.request.max.bytes"         = "104857600"
        "log.retention.hours"              = "168"
        "log.segment.bytes"                = "1073741824"
        "log.cleanup.policy"               = "delete"
        "compression.type"                 = "snappy"
      }
    })
  ]

  depends_on = [azurerm_kubernetes_cluster.main]
}

# Spark Operator deployment
resource "helm_release" "spark_operator" {
  name             = "spark-operator"
  repository       = "https://googlecloudplatform.github.io/spark-on-k8s-operator"
  chart           = "spark-operator"
  namespace       = "spark-operator"
  create_namespace = true

  values = [
    yamlencode({
      sparkJobNamespace = "spark-jobs"
      
      # Enable metrics
      metrics = {
        enable = true
        port   = 10254
      }
      
      # Webhook configuration
      webhook = {
        enable = true
        port   = 8080
      }
      
      # Resource allocation
      resources = {
        limits = {
          memory = "512Mi"
          cpu    = "500m"
        }
        requests = {
          memory = "256Mi"
          cpu    = "250m"
        }
      }
    })
  ]

  depends_on = [azurerm_kubernetes_cluster.main]
}

# Prometheus and Grafana for monitoring
resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart           = "kube-prometheus-stack"
  namespace       = "monitoring"
  create_namespace = true

  values = [
    yamlencode({
      prometheus = {
        prometheusSpec = {
          storageSpec = {
            volumeClaimTemplate = {
              spec = {
                storageClassName = "managed-premium"
                accessModes      = ["ReadWriteOnce"]
                resources = {
                  requests = {
                    storage = "50Gi"
                  }
                }
              }
            }
          }
          retention = "15d"
        }
      }
      
      grafana = {
        persistence = {
          enabled      = true
          storageClassName = "managed-premium"
          size         = "10Gi"
        }
        
        # Default admin credentials
        adminPassword = "admin123!"
        
        # Grafana configuration
        grafana.ini = {
          server = {
            domain = var.grafana_domain
          }
        }
      }
    })
  ]

  depends_on = [azurerm_kubernetes_cluster.main]
}
