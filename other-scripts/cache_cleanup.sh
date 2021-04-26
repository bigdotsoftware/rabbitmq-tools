#!/usr/bin/env bash
set -euo pipefail

#----------- Configuration -------------
RABBIT_USER=guest
RABBIT_PASS=guest
RABBIT_HOST=http://localhost:15672

#------------------ Code ----------------
# 
# RabbitMQ < 3.6.2
# > rabbitmqctl eval 'exit(erlang:whereis(rabbit_mgmt_db), please_terminate).'
#
# RabbitMQ > 3.6.2 and RabbitMQ < 3.6.7
# > rabbitmqctl eval 'supervisor2:terminate_child(rabbit_mgmt_sup_sup, rabbit_mgmt_sup), rabbit_mgmt_sup_sup:start_child().'
# 
# RabbitMQ >= 3.6.7 (current node)
# > rabbitmqctl eval 'rabbit_mgmt_storage:reset().'
# 
# RabbitMQ >=3.6.2 (all nodes)
# > rabbitmqctl eval 'rabbit_mgmt_storage:reset_all().'
#

get_nodes() {
   curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/nodes/" -H "Content-Type: application/json" | jq -r '.[] | .name'
}
 
process_stats_restart() {
   #alternatively use DELETE /api/reset/:node for a single node
   curl -XDELETE -u $RABBIT_USER:$RABBIT_PASS "$RABBIT_HOST/api/reset" -H "Content-Type: application/json"
}

process_stats_restart
