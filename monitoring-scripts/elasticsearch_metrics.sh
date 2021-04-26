#!/usr/bin/env bash
set -euo pipefail

#############################################
# TODO
# - extract totals from /api/overview
# - check Elasticsearch request status + retry
# - nodes and queues filtering
# - if no filtering enabled, then don't request node and queue one-by-one, just request once and iterate through json
#############################################

#----------- Configuration -------------
RABBIT_USER=guest
RABBIT_PASS=guest
RABBIT_HOST=http://localhost:15672

ES_INDEX=metrics
ES_TYPE=_doc
ES_HOST=localhost:9200

#------------------ Code ----------------
urlencode() {
    # urlencode <string>

    #old_lc_collate=$LC_COLLATE
    #LC_COLLATE=C

    local length="${#1}"
    for (( i = 0; i < length; i++ )); do
        local c="${1:$i:1}"
        case $c in
            [a-zA-Z0-9.~_-]) printf '%s' "$c" ;;
            *) printf '%%%02X' "'$c" ;;
        esac
    done

    #LC_COLLATE=$old_lc_collate
}
get_nodes() {
   curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/nodes/" -H "Content-Type: application/json" | jq -r '.[] | .name'
}
get_queues() {
   curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/queues/" -H "Content-Type: application/json" | jq -r '.[] | .vhost + "%00" + .name'
}
get_encoded_queues() {
   for queue in $(get_queues); do
      read -r vhost name <<<$(IFS="%00"; echo $queue)
      encoded=$(urlencode "$vhost")
      echo $encoded/$name
   done
}
      
process_nodes_stats() {
   for node in $(get_nodes); do
   
      timestamp=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
      
      memory=`curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/nodes/${node}/memory"`
      MEMORY_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "memory" \
                  --arg node "$node" \
                  '{timestamp: $timestamp, type: $t, node: $node, memory: '"$memory"'}' )
      
      general_info=`curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/nodes/${node}"`
      
      partitions=`echo $general_info | jq -r '.partitions'`
      PARTITION_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "partitions" \
                  --arg node "$node" \
                  '{timestamp: $timestamp, type: $t, node: $node, partitions: '"$partitions"'}' )

      uptime=`echo $general_info | jq -r '.uptime'`
      UPTIME_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "uptime" \
                  --arg node "$node" \
                  --arg uptime "$uptime" \
                  '{timestamp: $timestamp, type: $t, node: $node, uptime: $uptime}' )

      disk=`echo $general_info | jq -r '{disk_free, disk_free_details, disk_free_limit, disk_free_alarm}'`
      DISK_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "disk" \
                  --arg node "$node" \
                  '{timestamp: $timestamp, type: $t, node: $node, disk: '"$disk"'}' )
                
      connections=`echo $general_info | jq -r '{connection_created, connection_created_details, connection_closed, connection_closed_details}'`
      CONNECTION_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "connections" \
                  --arg node "$node" \
                  '{timestamp: $timestamp, type: $t, node: $node, connections: '"$connections"'}' )
                
      channels=`echo $general_info | jq -r '{channel_created, channel_created_details, channel_closed, channel_closed_details}'`
      CHANNEL_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "channels" \
                  --arg node "$node" \
                  '{timestamp: $timestamp, type: $t, node: $node, channels: '"$channels"'}' )
                  
       echo $MEMORY_JSON
       echo $PARTITION_JSON
       echo $UPTIME_JSON
       echo $DISK_JSON
       echo $CONNECTION_JSON
       echo $CHANNEL_JSON
       
       out=`curl -s -X POST "$ES_HOST/_bulk?pretty" -H 'Content-Type: application/x-ndjson' -d'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$MEMORY_JSON"'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$PARTITION_JSON"'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$UPTIME_JSON"'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$DISK_JSON"'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$CONNECTION_JSON"'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$CHANNEL_JSON"'
'`
      errors=`echo $out | jq -r '.errors'`
      [[ "$errors" == "true" ]] && { echo "Processed with errors: $out"; exit 1; }
      echo "Node stats processed OK"
      
   done
}

process_queues_stats() {
   for vhostqueue in $(get_encoded_queues); do
   
      timestamp=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
      
      general_info=`curl -s -u $RABBIT_USER:$RABBIT_PASS -XGET "$RABBIT_HOST/api/queues/${vhostqueue}"`
      
      metrics=`echo $general_info | jq -r '{messages, messages_details, memory, message_bytes, vhost, node, name}'`
      queuename=`echo $metrics | jq -r '.name'`
      
      DETAILS_JSON=$( jq -c -n \
                  --arg timestamp "$timestamp" \
                  --arg t "queues" \
                  --arg queue "$queuename" \
                  '{timestamp: $timestamp, type: $t, queue: $queue, metrics: '"$metrics"'}' )
                  
      echo $DETAILS_JSON

      out=`curl -s -X POST "$ES_HOST/_bulk?pretty" -H 'Content-Type: application/x-ndjson' -d'
{ "index" : { "_index" : "'"$ES_INDEX"'", "_type" : "'"$ES_TYPE"'" } }
'"$DETAILS_JSON"'
'`
      errors=`echo $out | jq -r '.errors'`
      [[ "$errors" == "true" ]] && { echo "Processed with errors: $out"; exit 1; }
      echo "Queue stats processed OK"
      
   done
}

process_nodes_stats
process_queues_stats
