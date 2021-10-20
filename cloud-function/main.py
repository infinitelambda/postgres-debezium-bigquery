from google.api_core import retry
from google.cloud import pubsub_v1
import json
import time
import os

def handler(req):
    project_id = os.environ.get('GOOGLE_PROJECT_ID')
    subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION_ID')

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    NUM_MESSAGES = 1000

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry=retry.Retry(deadline=300),
        )

        ack_ids = []
        insert = []
        delete = []
        ans = None
        conf_ans = {}
        print(len(response.received_messages))
        for received_message in response.received_messages:
            pks_lst = json.loads(received_message.message.ordering_key).get("schema").get("fields")
            pks = [item["field"] for item in pks_lst if item["field"] != "__dbz__physicalTableIdentifier"]
            req = json.loads(received_message.message.data)
            table = req["payload"]["source"]["table"]
            conf_ans[table] = {"pk": {"primary_key": pks}}
            

            if req["payload"]["op"] == "u":
                insert.append(req["payload"]["after"])
                conf_ans[table].update({"insert": insert})
            if req["payload"]["op"] == "r":
                insert.append(req["payload"]["after"])
                conf_ans[table].update({"insert": insert})
            if req["payload"]["op"] == "c":
                insert.append(req["payload"]["after"])
                conf_ans[table].update({"insert": insert})
            if req["payload"]["op"] == "d":
                delete.append(req["payload"]["after"])
                conf_ans[table].update({"delete": delete})
            
            # Send JSON response back to Fivetran
            # Get the seconds since epoch
            secondsSinceEpoch = time.time()
            timeObj = time.localtime(secondsSinceEpoch)
            t = '%d-%d-%dT%d:%d:%dZ' % (
            timeObj.tm_year, timeObj.tm_mon, timeObj.tm_mday, timeObj.tm_hour, timeObj.tm_min, timeObj.tm_sec)

            ack_ids.append(received_message.ack_id)

        ans = {
            "state": {
                "transactionsCursor": t
            },
            "schema" : {
            },
            "insert": {
            },
            "delete": {
            },
            "hasMore" : False
        }

        for item in conf_ans.keys():
            ans["schema"][item] = conf_ans[item]["pk"]
            ans["insert"][item] = conf_ans[item].get("insert", [])
            ans["delete"][item] = conf_ans[item].get("delete", [])

            
        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

        print(
            f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        )

    return ans