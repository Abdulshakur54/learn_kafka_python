from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from config import config

def create_topic(admin_client, topic):
    new_topic = NewTopic(topic, num_partitions = 6, replication_factor = 3)
    features_dic = admin_client.create_topics([new_topic])
    for topic_name in features_dic.keys():
        if topic == topic_name:
            print(f"Topic {topic_name} have been created")
        else:
            print(f"An error occured, topic {topic_name} was not created")
            

def topic_exists(admin_client, topic) -> bool:
    cluster_meta_data = admin_client.list_topics()
    topic_meta_datas = cluster_meta_data.topics.values()
    for tmd in topic_meta_datas:
        if tmd.topic == topic:
            return True
    return False


def get_max_size(admin_client, topic):
    resource = ConfigResource('Topic', topic)
    features_dict = admin_client.describe_configs([resource])
    config_entries = features_dict[resource].result()
    max_size = config_entries['max.message.bytes']
    return max_size.value


# set max.message.bytes for topic
def set_max_size(admin, topic, max_k):
    config_dict = {'max.message.bytes': str(max_k*1024)}
    resource = ConfigResource('topic', topic, config_dict)
    result_dict = admin.incremental_alter_configs([resource])
    result_dict[resource].result()



if __name__ == '__main__':

    # Create Admin client
    admin = AdminClient(config)
    topic_name = 'my_topic'
    max_msg_k = 50

    # Create topic if it doesn't exist
    if not topic_exists(admin, topic_name):
        create_topic(admin, topic_name)

    # Check max.message.bytes config and set if needed
    current_max = get_max_size(admin, topic_name)
    if current_max != str(max_msg_k * 1024):
        print(f'Topic, {topic_name} max.message.bytes is {current_max}.')
        set_max_size(admin, topic_name, max_msg_k)

    # Verify config was set 
    new_max = get_max_size(admin, topic_name)
    print(f'Now max.message.bytes for topic {topic_name} is {new_max}')