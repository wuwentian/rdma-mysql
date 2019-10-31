# _*_coding:utf-8_*_
import pika
import json
import mnist_client as mc
import client_test1 as ct
import numpy as np
import k8s_ctl
from kubernetes.client.rest import ApiException
import appconfig
import hyper_param_search_lib
import os
import scipy.misc
import sys
import time
from pika.exceptions import ConnectionClosed
from PIL import Image



app_config = appconfig.app_config

user = str(app_config['user'])
password = str(app_config['password'])
credentials = pika.PlainCredentials(user,password)

# def rabbitmq_connection(self):
while True:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            str(app_config['rabbitmq_host']),str(app_config['rabbitmq_port']),'/',credentials))
        break
    except Exception as e:
        print('exception', e)
        time.sleep(3)

channel = connection.channel()
channel.queue_declare(queue=str(app_config['queue']))

def reconnection_mq():
    global connection
    global channel
    retry_time = 0
    while True:
        try:
            print('the %d time to try reconnecting'%retry_time)
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                str(app_config['rabbitmq_host']), str(app_config['rabbitmq_port']), '/', credentials))
            channel = connection.channel()
            channel.queue_declare(queue=str(app_config['queue']))
            break
        except ConnectionClosed as e:
            retry_time = retry_time + 1
            time.sleep(3)
    channel.basic_consume(queue_callback,
                          queue=str(app_config['queue']),
                          no_ack=True)
    channel.start_consuming()
#send mq message
def send_msg(queue, msg):
    # channel.queue_declare(queue=queue)
    channel.queue_declare(queue=queue , auto_delete=True)
    body = json.dumps(msg)
    print(queue, body)
    channel.basic_publish(exchange='',
                            routing_key=queue,
                            body=body
                             )

def start_train(msg):
    print(msg)
    error = 'error'
    ret = None
    if (msg['body']['num_ps'] >= 1 ) and (msg['body']['num_worker'] >= 1):
        try:
            ret = k8s_ctl.do_start_cluster_tfjob(str(msg['body']['train_instance_id']),
                                           msg['body']['num_ps'],
                                           msg['body']['num_worker'],
                                           msg['body']['home_dir'],
                                           msg['body']['work_dir'],
                                           msg['body']['script_main'],
                                           msg['body']['data_set_path'],
                                           msg['body']['model_dir'],
                                           msg['body']['train_type'],
                                           msg['body']['epoch_num'],
                                           msg['body']['leaning_rate'],
                                           msg['body']['momentum'] if msg['body'].has_key('momentum') else 0.01,
                                           msg['body']['decay_rate'] if msg['body'].has_key('decay_rate') else 0.01,
                                           msg['body']['batch_size'],
                                           msg['body']['params'],
                                           msg['body']['tensorboard'],
                                           msg['body']['image'],
                                           envs=None,
                                           limits_input={'ps': {'cpu': msg['body']['ps_cpu_request'],
                                                          'memory': str(msg['body']['ps_memory_request']) + 'Mi',
                                                          'rdma/vhca':4},
                                                         'worker':{'cpu': msg['body']['worker_cpu_request'],
                                                             'memory': str(msg['body']['worker_memory_request']) + 'Mi',
                                                             'alpha.kubernetes.io/nvidia-gpu': msg['body']['worker_gpu_request'],
                                                            'rdma/vhca': 4,
                                                             }
                                                    }
                                           )
        except ApiException as e:
            error = e.reason
            ret = False
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            return
    elif (msg['body']['num_ps'] == 0)  and  (msg['body']['num_worker'] == 1) :
        try:
            ret = k8s_ctl.do_start_tfjob_train(str(msg['body']['train_instance_id']),
                                           msg['body']['home_dir'],
                                           msg['body']['work_dir'],
                                           msg['body']['script_main'],
                                           msg['body']['data_set_path'],
                                           msg['body']['model_dir'],
                                           msg['body']['train_type'],
                                           msg['body']['epoch_num'],
                                           msg['body']['leaning_rate'],
                                           msg['body']['momentum'] if msg['body'].has_key('momentum') else 0.01,
                                           msg['body']['decay_rate'] if msg['body'].has_key('decay_rate') else 0.01,
                                           msg['body']['batch_size'],
                                           msg['body']['params'],
                                           msg['body']['tensorboard'],
                                           msg['body']['image'],
                                           envs=None,
                                           limits={'cpu': msg['body']['worker_cpu_request'],
                                                   'memory': str(msg['body']['worker_memory_request']) + 'Mi' ,
                                                   'alpha.kubernetes.io/nvidia-gpu': msg['body']['worker_gpu_request']
                                                   })

        except ApiException as e:
            error = e.reason
            ret = False
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            return

    else:
        ret = False
        error = 'error input'

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['train_instance_id'] = msg['body']['train_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is False:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        if (msg['body']['tensorboard']) is True:
            print('tensorboard', ret)
            resp['body']['tensorboard'] = ret[1]
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        # resp['body']['pod_name'] = ret[2][1]['name']
        # resp['body']['namespace'] = ret[2][1]['namespace']
    send_msg(resp_queue, resp)


def stop_train(msg):
    print(msg)
    error = 'can not delete'
    try:
       ret = k8s_ctl.do_stop_tfjob(str(msg['body']['train_instance_id']))
       print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        ret = False
        print('exception-error',e)

    try:
       ret = k8s_ctl.do_stop_cluster_tfjob(str(msg['body']['train_instance_id']))
       print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        ret = False
        print('exception-error',e)

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['train_instance_id'] = msg['body']['train_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is False:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        
    send_msg(resp_queue, resp)
                      
def get_train_info(msg):
    print(msg)
    ret = None
    error='error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
         ret=k8s_ctl.get_tfjob_state(str(msg['body']['train_instance_id']))
    except ApiException as e:
        print('api exception', e)
        ret = None
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message', e)
        ret = None

    resp['body']['method'] = 'response'
    resp['body']['train_instance_id'] = msg['body']['train_instance_id']
    resp['body']['command'] = msg['body']['command']
    
    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['progress_percent'] = ret['rate']
        resp['body']['status'] = ret['describe']
        if resp['body']['status'] =='ERROR':
            resp['body']['cause_code'] = ret['cause_code']
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

        if  resp['body']['status'] =='ERROR' and ret['cause_code'] == 'NOT-EXIST':
            error = 'error'
            try:
                id = 'cluster-'+str(msg['body']['train_instance_id'])+'-worker-0'
                print('distributed pod id =',id)
                ret = k8s_ctl.get_tfjob_state(id)
                print(ret)
            except ApiException as e:
                print('api exception', e)
                error = e.reason
                ret = False
            except Exception as e:
                print('exception', e)
                return

            if ret is None:
                resp['body']['success'] = False
                resp['body']['message'] = error
            else:
                resp['body']['progress_percent'] = ret['rate']
                resp['body']['status'] = ret['describe']
                resp['body']['cause_code'] = ''
                if resp['body']['status'] =='ERROR':
                    resp['body']['cause_code'] = ret['cause_code']
                resp['body']['success'] = True
                resp['body']['message'] = 'ok'

        
        send_msg(resp_queue, resp)
           
def start_model_test(msg):
    print(msg)
    error = 'error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    ret = None
    try:
        ret = k8s_ctl.do_start_tfjob_test(str(msg['body']['model_test_instance_id']),
                                     msg['body']['home_dir'],
                                     msg['body']['work_dir'],
                                     msg['body']['script_main'],
                                     msg['body']['data_set_path'],
                                     msg['body']['model_path'],
                                     msg['body']['params'],
                                     msg['body']['type'],
                                     msg['body']['image'],
                                     envs=None,
                                     limits={'cpu': msg['body']['cpu_request'],
                                             'memory': str(msg['body']['memory_request']) + 'Mi'},
                                     variable = {'topk_num':msg['body']['topk_num']},
                                          )
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message', e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return

    resp['body']['method'] = 'response'
    resp['body']['model_test_instance_id'] = msg['body']['model_test_instance_id']
    resp['body']['command'] = msg['body']['command']
    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)

def stop_model_test(msg):
    print(msg)
    error='can not delete'
    ret=None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']

    try:
        ret = k8s_ctl.do_stop_tfjob(str(msg['body']['model_test_instance_id']))
        print(ret)#add do_
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message', e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return

    resp['body']['method'] = 'response'
    resp['body']['model_test_instance_id'] = msg['body']['model_test_instance_id']
    resp['body']['command'] = msg['body']['command']
    
    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        
    send_msg(resp_queue, resp)
    
def get_model_test_info(msg):
    print(msg)
    error='error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        ret = k8s_ctl.get_tfjob_state(str(msg['body']['model_test_instance_id']))
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message',e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return
    

    resp['body']['method'] = 'response'
    resp['body']['model_test_instance_id'] = msg['body']['model_test_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
       
    else:
        resp['body']['progress_percent'] = ret['rate']
        resp['body']['current_round'] = ret['state']
        resp['body']['status'] = ret['describe']
        if resp['body']['status'] =='ERROR':
            resp['body']['cause_code'] = ret['cause_code']
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
            
    send_msg(resp_queue, resp)
      
def start_serving(msg):
    print(msg)
    error = 'error'
    try:
        ret = k8s_ctl.do_start_serving(str(msg['body']['start_serving_id']),
                           msg['body']['model_name'], 
                           msg['body']['model_path'],
                           msg['body']['image'],
                           replicas=1,
                           limits={'cpu': msg['body']['cpu_request'],
                                   'memory': str(msg['body']['memory_request']) + 'Mi'})
        print('ret',ret)
    except ApiException as e:
        error = e.reason
        ret = (False,None)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = (False,None)
    
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['start_serving_id'] = msg['body']['start_serving_id']
    resp['body']['command'] = msg['body']['command']
    if ret[0] is False:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['serving_id'] = ret[1]

    send_msg(resp_queue, resp)

def stop_serving(msg):
    print(msg)
    error = 'can not stop this serving'
    try:
        ret = k8s_ctl.do_stop_serving(str(msg['body']['stop_serving_id']))
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        return
        
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['stop_serving_id'] = msg['body']['stop_serving_id']
    resp['body']['command'] = msg['body']['command']
    if ret is True:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp) 
    
def start_inference(msg):
    error = 'can not start inference'
    try:
        hostport = k8s_ctl.get_serving_info(msg['body']['service_id'])
        print(hostport)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        return
    # num_1 = np.array(msg['body']['data_set'])
    image_num = int(msg['body']['image_num'])
    image_size_x = int(msg['body']['image_size_x'])
    image_size_y = int(msg['body']['image_size_y'])
    image_channel = int(msg['body']['image_channel'])
    image_shape = None
    if image_channel == 1:
        image_shape = [image_size_x, image_size_y]
    elif image_channel == 3:
        image_shape = [1,image_size_x, image_size_y, image_channel]
    image = msg['body']['data_set']
    image = np.array(image, dtype='float32')

    try:
        ret = mc.do_inference(hostport,
                              # [msg['body']['data_set']],
                              [image],
                              str(msg['body']['model_name']),
                              str(msg['body']['signature_name']),
                              image_shape,
                              concurrency = 1,
                              num_tests = image_num,
                              )
        print('do inference ret', ret)
    except ApiException as e:
        error = e.reason
        ret = None
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = None

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    # resp['body']['inference_instance_id'] = msg['body']['inference_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is not None:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['inference_result'] = ret
        
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error
    
    send_msg(resp_queue, resp)


def start_inference_param(msg):
    error = 'can not start inference'
    classify = None
    hostport = k8s_ctl.get_serving_info(msg['body']['service_id'])
    print(hostport)

    file_address = '/data1/' + str(msg['body']['image_name'])
    #get image
    # type_list = json.loads(msg['body']['type_list'])#[-1,28,28,1]
    type_list = msg['body']['type_list']
    type_list = np.array(type_list)
    print(type_list, type(type_list))
    x_size = type_list[1]
    y_size = type_list[2]
    type_clour = 1
    type_shape = None
    lens1 = len(type_list)
    if lens1 == 4:
        type_clour = int(type_list[3])
        type_shape = type_list
    image_input = scipy.misc.imread(file_address)
    lens2 = len(image_input.shape)
    print('image_input.shape', image_input.shape)
    if x_size<=0 or y_size<=0:
        ret = None
        error = 'image-size error'
    else:
        if lens2 == 3 and type_clour == 1:
            image_input = scipy.misc.imread(file_address, mode='L')
            image_input = np.array(image_input, dtype='float32')

        elif lens2 == 3 and type_clour == 3:
            print('>>>>>>>>>>>>>>>>come here this is cifar10 true clour')
            image_input = Image.open(file_address)
            image_input = np.array(image_input).astype(np.float32)
            # image_input = image_input.transpose((2, 0, 1))
            #image_input = image_input / 255.0
        # image_input = scipy.misc.imresize(image_input, (x_size, y_size))
        image = np.array(image_input, dtype='float32')
        image_shape =list(image.shape)
        if lens1 ==3:
            type_shape = image_shape
        print('image shape ', image, image_shape, image.dtype)
        try:
            ret = mc.do_inference(hostport,
                                  [image],
                                  str(msg['body']['model_name']),
                                  str(msg['body']['signature_name']),
                                  type_shape,
                                  concurrency=1,
                                  num_tests=1
                                  )

        except ApiException as e:
            error = e.reason
            ret = None
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            print('error',e)
            ret = None

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['command'] = msg['body']['command']

    if ret is not None:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['inference_result'] = ret
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)

def start_jupyter(msg):
    print(msg)
    error = 'error'
    try:
        ret = k8s_ctl.do_start_jupyter(str(msg['body']['jupyter_instance_id']),
                                        msg['body']['work_dir'],
                                        {'cpu':1, 'memory':'2Gi'})
        print(ret)
    except ApiException as e:
        error = e.reason
        print('error-1',error)
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-2',e)
        ret = False

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['jupyter_instance_id'] = msg['body']['jupyter_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is False or ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error

    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['jupyter_url'] = ret

    send_msg(resp_queue, resp)

def stop_jupyter(msg):
    print(msg)
    error = 'can not stop this jupyter'
    try:
        ret = k8s_ctl.do_stop_jupyter(str(msg['body']['jupyter_instance_id']))
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = False

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['jupyter_instance_id'] = msg['body']['jupyter_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is True:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)

def start_export(msg):
    print(msg)
    error = 'can not export'
    try:
        ret = k8s_ctl.do_start_tfjob_export(str(msg['body']['export_instance_id']),
                                            msg['body']['home_dir'],
                                            msg['body']['work_dir'],
                                            msg['body']['script_main'],
                                            msg['body']['model_path'],
                                            msg['body']['export_model_path'],
                                            msg['body']['version'],
                                            msg['body']['params'],
                                            msg['body']['image'])
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = None
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = None

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['export_instance_id'] = msg['body']['export_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is not None:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)

def stop_export(msg):
    print(msg)
    error = 'can not delete'
    try:
        ret = k8s_ctl.do_stop_tfjob(str(msg['body']['export_instance_id']))
        print(ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('exception',e)
        ret = False
    #info = k8s_ctl.get_tfjob_state(str(msg['body']['export_instance_id']))
    #print(info)
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['export_instance_id'] = msg['body']['export_instance_id']
    resp['body']['command'] = msg['body']['command']
    #resp['body']['progress_percent'] = info['rate']
    #resp['body']['current_round'] = info['state']
    #resp['body']['status'] = info['describe']

    if ret is True:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)

def get_export_info(msg):
    print(msg)
    error = 'error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        ret = k8s_ctl.get_tfjob_state(str(msg['body']['export_instance_id']))
        print('ret',ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('Exception', e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return


    resp['body']['method'] = 'response'
    resp['body']['export_instance_id'] = msg['body']['export_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['progress_percent'] = ret['rate']
        resp['body']['status'] = ret['describe']
        if resp['body']['status'] =='ERROR':
            resp['body']['cause_code'] = ret['cause_code']
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)


def get_serving_info_2(msg):
    print(msg)
    ret = None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    error = 'error'
    try:
        ret = k8s_ctl.get_serving_info_2(msg['body']['service_id'])
        print('ret', ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('Exception', e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return

    resp['body']['method'] = 'response'
    resp['body']['service_id'] = msg['body']['service_id']
    resp['body']['command'] = msg['body']['command']

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['status'] = ret['describe']
        if resp['body']['status'] == 'ERROR':
            resp['body']['cause_code'] = ret['cause_code']
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)

def container_create(msg):
    print(msg)
    error = 'error'
    port = [80,81]
    try:
        enableDesktop = msg['body']['enable_desktop']
        if enableDesktop is None:
            raise Exception('invaild field')
        if enableDesktop:
            ret = k8s_ctl.do_start_tfjob_container(str(msg['body']['container_create_instance_id']),
                                                   msg['body']['image'],
                                                   msg['body']['home_dir'],
                                                   limits={'cpu': msg['body']['cpu_request'],
                                                           'memory': str(msg['body']['memory_request']) + 'Mi'},
                                                   port=port)
        else:
            ret = k8s_ctl.do_start_tfjob_container(str(msg['body']['container_create_instance_id']),
                                                   msg['body']['image'],
                                                   msg['body']['home_dir'],
                                                   limits={'cpu': msg['body']['cpu_request'],
                                                           'memory': str(msg['body']['memory_request']) + 'Mi'},
                                                   port = port[1])

    except ApiException as e:
        error = e.reason
        ret = None
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = None

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['container_create_instance_id'] = msg['body']['container_create_instance_id']
    resp['body']['command'] = msg['body']['command']

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)

def container_delete(msg):
    print(msg)
    error = 'can not stop this container'
    try:
        ret = k8s_ctl.do_stop_container(str(msg['body']['container_stop_instance_id']))
        print('container_del_info',ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        ret = False

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['container_stop_instance_id'] = msg['body']['container_stop_instance_id']
    resp['body']['command'] = msg['body']['command']
    if ret is True:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)

def get_container_info(msg):
    print(msg)
    ret = None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    error = 'container does not exit'
    try:
        ret = k8s_ctl.get_container_info(str(msg['body']['container_instance_id']))
        print('container_info',ret)
    except ApiException as e:
        error = e.reason
        ret = False
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message',e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return

    resp['body']['method'] = 'response'
    resp['body']['container_instance_id'] = msg['body']['container_instance_id']
    resp['body']['command'] = msg['body']['command']
    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
    else:
        resp['body']['host_ip'] = ret['host_ip']
        resp['body']['pod_ip'] = ret['pod_ip']
        resp['body']['node_name'] = ret['node_name']
        resp['body']['status'] = ret['describe']
        resp['body']['host_port'] = ret['host_port']
        resp['body']['host_port1'] = ret['host_port1']
        if resp['body']['status'] == 'ERROR' or resp['body']['status'] == 'Pending':
            resp['body']['cause_code'] = ret['cause_code']
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)


def optimization_advice(msg):
      print(msg)
      try:
          ret = k8s_ctl.optimization_advice(msg['body']['train_accurate_rate'],
                                          msg['body']['test_accurate_rate'])
      except ConnectionClosed as e:
          raise e
      except Exception as e:
          ret = None

      resp = {'head': msg['head'], 'body': {}}
      resp_queue = msg['head']['reply_queue']
      resp['body']['method'] = 'response'
      resp['body']['command'] = msg['body']['command']

      if ret is None:
          resp['body']['success'] = False
          resp['body']['message'] = 'error'
      else:
          resp['body']['response'] = ret
          resp['body']['success'] = True
          resp['body']['message'] = 'ok'

      send_msg(resp_queue, resp)


def get_gpu_info(msg):
    print(msg)
    ret = None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        ret = k8s_ctl.get_gpu_statis(msg['body']['pod_name'])
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message',e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        send_msg(resp_queue, resp)
        return
    resp['body']['method'] = 'response'
    resp['body']['command'] = msg['body']['command']

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['gpu_info'] = ret
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)


def hyper_param_search(msg):
    print(msg)
    ret = []
    params = msg['body']['params']
    if msg['body']['search_type'] == 'random_search' :
        try:
            for param in hyper_param_search_lib.random_search(params):
                ret.append(param)
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            return
    elif msg['body']['search_type'] == 'grid_search' :
        try:
            for param in hyper_param_search_lib.grid_search(params):
                ret.append(param)
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            return
    # elif msg['body']['search_type'] == 'BayesOptimizer' :
    #     try:
    #         bo = hyper_param_search_lib.BayesOptimizer(params)
    #         param = bo.next()
    #         bo.add_tried_param(param, score)
    #         #for param in bo:
    #         #    ret.append(param)
    #         #    bo.add_tried_param(param, score)
    #     except Exception as e:
    #         return

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['command'] = msg['body']['command']
    print('ret',ret)

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['search_result'] = ret
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)



def get_node_statis(msg):
    print(msg)
    ret = None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        ret = k8s_ctl.get_node_statis(str(msg['body']['node_name']))
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error-message',e)
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
        resp['body']['result'] = []
        send_msg(resp_queue, resp)
        return
    resp['body']['method'] = 'response'
    resp['body']['command'] = msg['body']['command']
    print('ret',ret)

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['result'] = ret
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)

def start_tensorboard(msg):
    print(msg)
    url = None
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        url = k8s_ctl.do_start_tensorboard(msg['body']['id'],
                                   msg['body']['home_dir'],
                                   msg['body']['model_dir'],
                                   limits=None)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        url = None

    if url is None:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['url'] = url
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)


def stop_tensorboard(msg):
    print(msg)
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    try:
        ret = k8s_ctl.do_stop_tensorboard(msg['body']['id'])
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error',e)
        ret = None

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = 'error'
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'

    send_msg(resp_queue, resp)
#add some new

# def delete_node(msg):
#     print(msg)
#     error = 'error'
#     resp = {'head':msg['head'], 'body':{}}
#     resp_queue = msg['head']['reply_queue']
#     node_list = list(msg['body']['node_names'])
#     try:
#         ret = k8s_ctl.do_delete_node(node_list)
#     except ApiException as e:
#         error = e.reason
#         ret = None
#     except Exception as e:
#         print('error', e)
#         ret = None
#
#     if ret is None:
#         resp['body']['success'] = False
#         resp['body']['message'] = error
#         resp['body']['result'] = ''
#     else:
#         resp['body']['success'] = True
#         resp['body']['message'] = 'ok'
#         resp['body']['result'] = ret
#
#     send_msg(resp_queue, resp)
#
# def create_node(msg):
#     print(msg)
#     error = 'error'
#     resp = {'head': msg['head'], 'body':{}}
#     resp_queue = msg['head']['reply_queue']
#     node_list = list(msg['body']['node_names'])
#     try:
#         ret = k8s_ctl.do_create_node(node_list)
#     except Exception as e:
#         print('error', e)
#         ret = None
#
#     if ret is None:
#         resp['body']['success'] = False
#         resp['body']['message'] = error
#         resp['body']['result'] = ''
#     else:
#         resp['body']['success'] = True
#         resp['body']['message'] = 'ok'
#         resp['body']['result'] = ret
#
#     send_msg(resp_queue, resp)

def patch_node_schedulable(msg):
    print(msg)
    error = 'error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    node_list = list(msg['body']['node_names'])
    try:
        ret = k8s_ctl.do_patch_node_schedulable(node_list)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error', e)
        ret = None

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
        resp['body']['result'] = ''
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['result'] = ret

    send_msg(resp_queue, resp)

def patch_node_unschedulable(msg):
    print(msg)
    error = 'error'
    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    node_list = list(msg['body']['node_names'])
    try:
        ret = k8s_ctl.do_patch_node_unschedulable(node_list)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error', e)
        ret = None

    if ret is None:
        resp['body']['success'] = False
        resp['body']['message'] = error
        resp['body']['result'] = ''
    else:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['result'] = ret

    send_msg(resp_queue, resp)


def do_inference_test(msg):
    error = 'can not start inference'
    hostport = k8s_ctl.get_serving_info(msg['body']['service_id'])
    print(hostport)

    image = './' + str(msg['body']['image_name'])

    try:
        ret = mc.do_inference(hostport,
                              image,
                              str(msg['body']['model_name']),
                              str(msg['body']['signature_name']),
                              image_shape=None,
                              concurrency=1,
                              num_tests=1
                              )

    except ApiException as e:
        error = e.reason
        ret = None
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('error', e)
        ret = None

    resp = {'head': msg['head'], 'body': {}}
    resp_queue = msg['head']['reply_queue']
    resp['body']['method'] = 'response'
    resp['body']['command'] = msg['body']['command']

    if ret is not None:
        resp['body']['success'] = True
        resp['body']['message'] = 'ok'
        resp['body']['inference_result'] = ret
    else:
        resp['body']['success'] = False
        resp['body']['message'] = error

    send_msg(resp_queue, resp)




def queue_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    msg = json.loads(body)
    print('msg', msg)
    if msg is None:
        return
    command = msg['body']['command']
    try:
        if command == 'start_train':
            start_train(msg)
        elif command == 'stop_train':
            stop_train(msg)
        elif command == 'get_train_info':
            get_train_info(msg)
        elif command == 'start_export':
            start_export(msg)
        elif command == 'stop_export':
            stop_export(msg)
        elif command == 'get_export_info':
            get_export_info(msg)
        elif command == 'start_model_test':
            start_model_test(msg)
        elif command == 'stop_model_test':
            stop_model_test(msg)
        elif command == 'get_model_test_info':
            get_model_test_info(msg)
        elif command == 'start_serving':
            start_serving(msg)
        elif command == 'stop_serving':
            stop_serving(msg)
        elif command == 'get_serving_info':
            get_serving_info_2(msg)
        elif command == 'do_inference':
             start_inference(msg)
        elif command == 'do_inference_param':
             start_inference_param(msg)
        elif command == 'start_jupyter':
             start_jupyter(msg)
        elif command == 'stop_jupyter':
             stop_jupyter(msg)
        elif command == 'container_create':
             container_create(msg)
        elif command == 'container_stop':
             container_delete(msg)
        elif command == 'get_container_info':
             get_container_info(msg)
        elif command == 'optimization_advice':
             optimization_advice(msg)
        elif command == 'get_gpu_info':
             get_gpu_info(msg)
        elif command == 'hyper_param_search':
             hyper_param_search(msg)
        elif command == 'get_node_info':
             get_node_statis(msg)
        elif command == 'start_tensorboard':
             start_tensorboard(msg)
        elif command == 'stop_tensorboard':
             stop_tensorboard(msg)
        elif command == 'patch_node_schedulable':
             patch_node_schedulable(msg)
        elif command == 'patch_node_unschedulable':
             patch_node_unschedulable(msg)
        elif command == 'do_inference_test':
             do_inference_test(msg)

        else:
            print('unkn own command:' + command)
    except ConnectionClosed as e:
        print('Try reconnecting rabbbitmq',e)
        reconnection_mq()

    sys.stdout.flush()

try:
    channel.basic_consume(queue_callback,
                          queue=str(app_config['queue']),
                          no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()
except ConnectionClosed as e:
    print('Try reconnecting rabbbitmq', e)
    reconnection_mq()



