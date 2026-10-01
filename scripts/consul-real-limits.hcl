# Local Consul-to-Spotter scale qualification only.
# Production Consul limits are deployment-owned and are not changed by this
# repository fixture.
data_dir = "/consul/data"

limits {
  http_max_conns_per_client = 10000
}
