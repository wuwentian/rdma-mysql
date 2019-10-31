from kubernetes import client, config
import time
#from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.client.models import V1DeleteOptions
import re
import tfjob
import appconfig
from influxdb import InfluxDBClient
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2
from grpc.framework.interfaces.face import face
from grpc.beta import implementations
from pika.exceptions import ConnectionClosed
#add data
app_config = appconfig.app_config


class Status(object):

    def __init__(self, status=None, reason=None):
        self.status = status
        self.reason = reason

    def __str__(self):
        """
        Custom error messages for exception
        """
        error_message = "({0})\n"\
                        "Reason: {1}\n".format(self.status, self.reason)

        return error_message

api_client = None
def get_api(version='CoreV1Api'):
    global api_client

    if api_client is None:
        #config.load_kube_config('./k8s_config')
        c = Configuration()
        c.verify_ssl = False
        #c.assert_hostname = False
        c.host = 'http://%s:%s' % (app_config['k8s_master_host'], app_config['k8s_master_port'])
        Configuration.set_default(c)
        api_client = client.ApiClient()

    if version == 'CoreV1Api':
        api = client.CoreV1Api(api_client)
    elif version == 'AppsV1beta1Api':
        api = client.AppsV1beta1Api(api_client)
    elif version == 'ExtensionsV1beta1Api':
        api = client.ExtensionsV1beta1Api(api_client)
    return api

def service_create(api, name, port):
    resp = None
    try:
        resp = api.read_namespaced_service(name=name,
                                    namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    if not resp:
        print("Service %s does not exits. Creating it..." % name)
        svc_manifest = {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {
                'name': name,
                'labels': {
                    'app': name
                }
            },
            'spec': {
                'type': 'NodePort',
                'ports': [{
                    'port': port,
                    'targetPort':port,
                    'name':'port1'
                }],
                'selector': {
                    'app': name
                }
            }
        }
#if port is array ,at least 2 ports prepared, else port is num, skip this branch
        try:
            if len(port)>1:
                svc_manifest['spec']['ports'][0]['port'] = port[0]
                svc_manifest['spec']['ports'][0]['targetPort'] = port[0]
                ports2 = {'port': port[1],'targetPort':port[1],'name':'port2'}
                svc_manifest['spec']['ports'].append(ports2)
        except Exception as e:
            print('this is number', e)
        try:
            resp = api.create_namespaced_service(body=svc_manifest,
                                        namespace='default')
        except Exception as e:
            print('exception',e)
        for i in range(10):
            resp = api.read_namespaced_service(name=name,
                                        namespace='default')
            if resp != None:
                break
            time.sleep(1)
        print("Done.")
    return resp

def get_volumes_dir(home_dir='/data', data_dir=None):
    volume_dirs = []
    volume_dirs.append({
        'name': 'storagedir1',
        'mountPath': '/data'
    })
    if data_dir != None:
        volume_dirs.append({
            'name': 'storagedir2',
            'mountPath': '/data1'
        })
    return volume_dirs

def get_volumes(home_dir='/data', data_dir=None):
    volumes = []
    if app_config['storage_type'] == 'nfs':
        volumes.append({
            'name': 'storagedir1',
            'nfs': {
                'server': app_config['nfs_host'],
                'path': app_config['nfs_base_path'] + home_dir[5:]  # skip head string /data/user1
            }
        })
        if data_dir != None:
            volumes.append({
                'name': 'storagedir2',
                'nfs': {
                    'server': app_config['nfs_host'],
                    'path': app_config['nfs_base_path'] + data_dir[5:]  # skip head string /data/user1
                }
            })
    elif app_config['storage_type'] == 'glusterfs':
        volumes.append({
            'name': 'storagedir1',
            'glusterfs': {
                'endpoints': 'glusterfs-cluster',
                'path': 'test-volume' + home_dir[5:]
            }
        })
        if data_dir != None:
            volumes.append({
                'name': 'storagedir2',
                'glusterfs': {
                    'endpoints': 'glusterfs-cluster',
                    'path': 'test-volume' + data_dir[5:]
                }
            })
    else:
        pass

    return volumes

def pod_obj_create(name, image, start_policy, command, args, envs, limits,
                   port, work_dir, home_dir='/data',data_dir=None):
    if work_dir :
        work_dir = '/data' + work_dir
    if port is None:
        port = 8888
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': name,
            'labels': {
                'app': name
            }
        },
        'spec': {

            'restartPolicy': start_policy,
            'containers': [{
                'image': image,
                'name': name,
                'command': command,
                'args': args,
                'env': envs,
                'resources': {
                    'limits': limits
                },
                'ports': [{
                    'containerPort': port
                }],
                'volumeMounts': get_volumes_dir(home_dir, data_dir),

                'workingDir': work_dir
            }],
            'volumes': get_volumes(home_dir, data_dir),
        }
    }
    if limits is not None:
        if limits.get('alpha.kubernetes.io/nvidia-gpu' , 0) > 0:
            volumeMountNvidia = {
                    'name': 'libnvidia',
                    'mountPath': '/usr/local/nvidia'
                }
            volumeNvidia = {
                'name': 'libnvidia',
                'hostPath': {
                    'path': '/usr/local/nvidia',
                }}
            pod_manifest['spec']['containers'][0]['volumeMounts'].append(volumeMountNvidia)
            pod_manifest['spec']['volumes'].append(volumeNvidia)

    if port is not None:
        try:
            if len(port)>1:
                pod_manifest['spec']['containers'][0]['ports'][0]['containerPort'] = port[0]
                ports1 = {'containerPort': port[0]}
                pod_manifest['spec']['containers'][0]['ports'].append(ports1)
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            print('this is number',e)

    return pod_manifest

def deploy_obj_create(name, replicas, pod_manifest):
    del pod_manifest['apiVersion']
    del pod_manifest['kind']
    deploy_manifest = {
		'apiVersion': 'extensions/v1beta1',
		'kind': 'Deployment',
		'metadata': {
            'name': name,
            'labels': {
                'app': name
            }
        },
        'spec': {
            'replicas': replicas,
            'selector': {
                'matchLabels': {
                    'app': name
                }
            },
            'template': pod_manifest
        }
    }
    return deploy_manifest

def pod_create(api, name, image, command=None, args=None, port=None,
               work_dir=None, envs=None, limits=None, home_dir='/data', data_dir=None):
    """
    envs: array of dict, eg, [{'name': name, 'value': value}]
    limits: limit of resource dict, eg, {'cpu': cpu, 'memory': memory}
    """
    resp = None
    try:
        resp = api.read_namespaced_pod(name=name,
                                    namespace='default')
        print('resp', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    except Exception as e:
        print('pod_create Exception', e)
    if not resp:
        print("Pod %s does not exits. Creating it..." % name)
        start_policy = 'Never'
        pod_manifest = pod_obj_create(name, image, start_policy, command,
                                      args, envs, limits, port, work_dir, home_dir, data_dir)
        #print('pod_manifest', pod_manifest)
        print('pod_manifest',pod_manifest)
        resp = api.create_namespaced_pod(body=pod_manifest,
                                        namespace='default')
        for i in range(5):
            resp = api.read_namespaced_pod(name=name,
                                        namespace='default')
            # if resp.status.phase != 'Pending':
            if resp is not None:
                break
            time.sleep(1)

        print("Done.")

    return resp

def deploy_create(api, name, image, replicas=1, command=None, args=None, port=None,
               work_dir=None, envs=None, limits=None, home_dir='/data', data_dir=None):
    """
    envs: array of dict, eg, [{'name': name, 'value': value}]
    limits: limit of resource dict, eg, {'cpu': cpu, 'memory': memory}
    """

    resp = None
    try:
        resp = api.read_namespaced_deployment(name=name,
                                    namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    if not resp:
        print("Deploy %s does not exits. Creating it..." % name)
        start_policy = 'Always'
        pod_manifest = pod_obj_create(name, image, start_policy, command,
                                      args, envs, limits, port, work_dir, home_dir, data_dir)
        deploy_manifest = deploy_obj_create(name, replicas, pod_manifest)
        print('deploy_manifest', deploy_manifest)
        resp = api.create_namespaced_deployment(body=deploy_manifest,
                                        namespace='default')
        for i in range(10):
            resp = api.read_namespaced_deployment(name=name,
                                        namespace='default')
            if resp is not None:
                break
            time.sleep(1)

        print("Done.")
    return resp

def deploy_delete(api, name):
    resp = None
    try:
        resp = deploy_scale(api, name, 0)
        print('deploy_scale', resp)
        resp = api.delete_collection_namespaced_replica_set('default', label_selector='app='+name)
        print('delete replica', resp)
        delete_options = client.V1DeleteOptions()
        print('delete_options', delete_options)
        resp = api.delete_namespaced_deployment(name, 'default', delete_options)
        print('delete deploy', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
        resp = None

    return resp

def deploy_scale(api, name, replicas):
    resp = None
    scale = {
        'metadata': {
            'name': name,
            'namespace': 'default'
        },
        'spec': { 'replicas': replicas}
    }
    for i in range(10):
        resp = None
        try:
            resp = api.replace_namespaced_deployment_scale(name, 'default', scale)
            if (resp.status.replicas == replicas):
                print('replicas ok to:', replicas)
                break
            time.sleep(1)
        except ConnectionClosed as e:
            raise e
        except ApiException as e:
            if e.status != 404:
                print("Unknown error: %s" % e)
            break
    return resp


# return instance of WSClient
def pod_attach(api, name):
    resp = None
    try:
        resp = stream(api.connect_get_namespaced_pod_attach, name, 'default',
                    stderr=True, stdin=True,
                    stdout=True, tty=False, _preload_content=False, _request_timeout=5)
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    return resp



# when _preload_content is True, return string type response
# when _preload_content is False, return instance of WSClient
def pod_exec(api, name, exec_command, _preload_content=True):
    resp = None
    try:
        resp = stream(api.connect_get_namespaced_pod_exec, name, 'default',
                    command=exec_command,
                    stderr=True, stdin=True,
                    stdout=True, tty=False, _preload_content=_preload_content)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    return resp


def pod_delete(api, name):
    resp = None
    try:
        delete_options = client.V1DeleteOptions(grace_period_seconds=1)
        print('delete_options', delete_options)
        resp = api.delete_namespaced_pod(name, 'default', delete_options)
        print('delete pod', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    return resp

def service_delete(api, name):
    resp = None
    try:
        resp = api.delete_namespaced_service(name, 'default')
        print('delete service', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    return resp

def do_start_jupyter(id, work_dir, limits):
    # Configs can be set in Configuration class directly or using helper utility
    url = None
    api = get_api()
    name = 'tensorflow-jupyter-' + id
    image = app_config['jupyter_image']
    command = ['/run_jupyter.sh', '--port=8888',  '--allow-root']
    args = None
    pod = pod_create(api, name, image, command, args, 8888, '/', None,
                     limits=limits, home_dir=work_dir, data_dir=None)
    if pod is None:
        return False

    for i in range(10):
        resp = api.read_namespaced_pod(name=name,
                                       namespace='default')
        if resp.status.phase != 'Pending':
            break
        time.sleep(1)
    print('pod-jupyter',resp)
    host = resp.status.host_ip
    # host = pod.status.host_ip
    print('node name:', host)
    if host is None:
        return False
    svc = service_create(api, name, 8888)
    if svc is None:
        pod_delete(api, name)
        return False
    port = svc.spec.ports[0].node_port
    print('svc port', port)
    # calling exec and wait for response.
    exec_command = [
        'jupyter',
        'notebook',
        'list']
    url = None
    for i in range(10):
        resp = pod_exec(api, name, exec_command)
        if resp:
            print("Response: " + resp)
            token = re.findall(r"token=(.+?) ", resp)
            if token and len(token) == 1:
                #url = "http://%s.ai:%d/?token=%s" % (host, port, token[0])
                url = "http://%s:%d/?token=%s" % (host, port, token[0])
                print('url:', url)
                break
        time.sleep(1)
        continue

    if url is None:
        do_stop_jupyter(id)
        return None

    return url

def do_stop_jupyter(id):
    name = 'tensorflow-jupyter-' + id
    api = get_api()
    resp = pod_delete(api, name);
    resp = service_delete(api, name);
    if resp is None:
        return False
    return True

def do_start_tfjob_train(id, home_dir, work_dir, script_main, data_dir,
                    model_dir, retrain, epoch=1, learning_rate=0.01, momentum=0.01, decay_rate=0.01, batch_size=100,
                    params='', cluster_num=1, tensorboard=False, image=None, envs=None, limits=None,
                    port=8888):
    if data_dir.find(home_dir) == 0:
        data = '/data'+data_dir[len(home_dir):]
    else:
         data = '/data1'
    num_gpus = 0
    if limits.get('alpha.kubernetes.io/nvidia-gpu', 0) > 0:
         num_gpus = limits.get('alpha.kubernetes.io/nvidia-gpu', 0)
    command = ['sh', '-c', ' python -u ' + script_main +
               ' --data_dir='+data +
               ' --model_dir=/data'+model_dir +
               ' --retrain=' + str(retrain) +
               ' --epoch_num='+str(epoch) +
               ' --num_gpus='+str(num_gpus) +
               ' --learning_rate='+str(learning_rate) +
               ' --momentum='+str(momentum) +
               ' --decay_rate='+str(decay_rate)+
               ' --batch_size='+str(batch_size) +
               ' --cluster_num='+str(cluster_num)+
               ' --id='+str(id) +
               ' ' + params +
               ' ']
               # ' >> console.log 2>&1']
    resp = do_start_tfjob(id, home_dir, work_dir, command, envs, limits, port , data_dir, image)
    if resp is False:
        return False, None
    url = None
    if tensorboard:
        url = do_start_tensorboard(id, home_dir, model_dir, limits)
        if url is None:
            do_stop_tfjob(id)
            return False, None
    return True, url , resp

def do_start_tfjob_test(id, home_dir, work_dir, script_main, data_dir,
                   model_dir, params='',type=None, image=None,
                   envs=None, limits=None, variable=None, port=8888):
    # if data_dir.index(home_dir) == 0:
    if data_dir.find(home_dir) == 0:
        data = '/data'+data_dir[len(home_dir):]
    else:
         data = '/data1'
    topk_num = 10
    if variable.get('topk_num', 10) > 0:
        topk_num = variable.get('topk_num', 10)

    command = ['sh', '-c', ' python -u ' + script_main +
               ' --data_dir='+data +
               ' --model_dir=/data'+model_dir +
               ' --topk_num='+str(topk_num) +
               ' --id='+str(id) +
               ' ' + params +
               ' ']
               # ' >> console.log 2>&1']
    return do_start_tfjob(id, home_dir, work_dir, command, envs, limits, port, data_dir,image)

def do_start_tfjob_export(id, home_dir, work_dir, script_main,
                   model_dir, export_dir, version=1, params='', image=None,
                   envs=None, limits=None, port=8888):
    command = ['sh', '-c', ' python -u ' + script_main +
               ' --model_dir=/data'+model_dir +
               ' --export_dir=/data'+export_dir +
               ' --version='+str(version) +
               ' ' + params +
               ' ']
               # ' >> console.log 2>&1']
    return do_start_tfjob(id, home_dir, work_dir, command, envs, limits, port, None, image)


def do_start_tfjob_container(id, image, home_dir,  limits=None, port=None):
    api = get_api()
    name = 'tf-container-' + id
    image = image
    args = None
    work_dir = '/'
    home_dir = home_dir
    command = None
    pod = pod_create(api, name, image, command, args, port,
                     work_dir, None, limits, home_dir)
    print(pod)
    if pod is None:
        return False
    # host = pod.spec.node_name
    # print('node name:', host)
    # port = pod.status.host_ip
    # print('host ip:', port)
    if port:
        svc = service_create(api, name, port)
        if svc is None:
            pod_delete(api, name)
            return False
        port = svc.spec.ports[0].node_port
        print('svc port', port)

    return True




#tfjob is train or test or export or any command
def do_start_tfjob(id, home_dir, work_dir, command, envs=None, limits=None,
                   port=8888 , data_dir=None, image=None):
    # Configs can be set in Configuration class directly or using helper utility
    api = get_api()
    name = 'tensorflow-job-' + id
    # image= app_config['tfjob_image']

    if image is None or image == '':
        image = app_config['tfjob_image']
        if limits is not None:
            if limits.get('alpha.kubernetes.io/nvidia-gpu', 0) >0:
                image = app_config['gpu_image']
    print('choose_images',image)
    args = None
    print(command)
    if envs == None:
        envs = []

    envs.append({'name': 'TFJOB_NAME', 'value': name})
    envs.append({'name': 'MONGODB_HOST', 'value': app_config['mongodb_host']})
    envs.append({'name': 'MONGODB_PORT', 'value': app_config['mongodb_port']})
    #add 4-26
    envs.append({'name': 'MONGODB_AIOS_HOST', 'value': app_config['mongodb_aios_host']})
    envs.append({'name': 'MONGODB_AIOS_PORT', 'value': app_config['mongodb_aios_port']})


    #envs.append({'name': 'PYTHONPATH', 'value': '/data/python/lib'})
    pod = pod_create(api, name, image, command, args, port,
                     work_dir, envs, limits, home_dir, data_dir)
    print(pod)
    if pod is None:
        return False
    for i in range(5):
        resp = api.read_namespaced_pod(name=name,
                                       namespace='default')
        if resp.status.phase != 'Pending':
            break
        time.sleep(1)
    print('resp',resp)
    host = resp.spec.node_name
    name = resp.metadata.name
    namespace = resp.metadata.namespace
    print('node name/namespace:', host, name, namespace)
    doc = {'name':name , 'namespace':namespace}
    # calling exec and wait for response.
    set_tfjob_state(id, 0, tfjob.DEPLOY)

    return True, doc

def do_stop_tfjob(id):
    name = 'tensorflow-job-' + id
    api = get_api()
    # if flag is True:
    # do_stop_tensorboard(id)
    print('stop-name',name)
    resp = pod_delete(api, name);
    if resp is None:
        return False
    else:
        return True

def do_stop_container(id):
    name = 'tf-container-' + id
    api = get_api()
    resp = pod_delete(api, name);
    resp = service_delete(api, name);
    if resp is None:
        return False
    else:
        return True


def do_start_tensorboard(id, home_dir, model_dir, limits=None):
    # Configs can be set in Configuration class directly or using helper utility
    api = get_api()
    name = 'tensorflow-tensorboard-' + id
    image = app_config['tensorboard_image']
    command = ['tensorboard', '--logdir=/data'+model_dir]
    limits = {'cpu': 1, 'memory': '1024Mi'}
    args = None
    print(command)
    port = 6006
    pod = None
    resp = None
    if limits.get('alpha.kubernetes.io/nvidia-gpu', 0) > 0:
        limits['alpha.kubernetes.io/nvidia-gpu'] = 0
    try:
        pod = pod_create(api, name, image, command, args, port,
                     model_dir, None, limits, home_dir)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    if pod is None:
        return None
    #host = pod.spec.node_name
    for i in range(10):
        resp = api.read_namespaced_pod(name=name,
                                       namespace='default')
        if resp.status.phase != 'Pending':
            break
        time.sleep(1)
    print('pod-tensorboard',resp)
    host = resp.status.host_ip
    print('host_ip', host)
    if host is None:
        return None
    # calling exec and wait for response.
    svc = None
    try:
        svc = service_create(api, name, port)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    if svc is None:
        pod_delete(api, name)
        return None
    port = svc.spec.ports[0].node_port
    print('svc port', port)
    return 'http://%s:%d/' % (host, port)

def do_stop_tensorboard(id):
    name = 'tensorflow-tensorboard-' + id
    api = get_api()
    try:
        resp = pod_delete(api, name);
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    try:
        resp = service_delete(api, name);
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
    if resp is None:
        return False
    else:
        return True

def set_tfjob_state(id, rate, state):
    name = 'tensorflow-job-' + id
    tfjob.set_state(name, rate, state)

def get_tfjob_state(id):
    name = 'tensorflow-job-' + id
    api = get_api()
    resp = None
    doc = {"tfjob_name": name, "rate": 0, "state": tfjob.ERROR, "describe": 'ERROR',"cause_code":''}
    try:
        resp = api.read_namespaced_pod(name=name,
                                namespace='default')
        #print('resp', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        print("Api Exception error-1: %s" % e)
        if e.status != 404:
            print("Unknown error-1: %s" % e)
        else:
            doc['cause_code'] = 'NOT-EXIST'
    except Exception as e:
        print('Exception-1',e)
    #
    # if resp:
    #     if resp.status.phase =='Succeeded' or resp.status.phase =='Running' :
    #         doc = tfjob.get_state(name)
    #         if doc['describe'] == 'ERROR':
    #             doc['cause_code'] = 'RunError'
    #     elif resp.status.phase == 'Pending':
    #         doc['describe'] = 'Pending'
    #         doc['cause_code'] = 'ResourceError'
    #     elif resp.status.phase == 'Failed' or resp.status.phase =='Unknown':
    #         doc['cause_code'] = 'PodError'

    if resp:
        if resp.status.phase == 'Succeeded' :
            doc = tfjob.get_state(name)
            if doc['describe'] == 'DEPLOY':
                set_tfjob_state(name, 1, tfjob.FINISH)
                doc['describe'] = 'FINISH'
                doc['rate'] = 1
        elif resp.status.phase =='Running' :
            doc = tfjob.get_state(name)
            if doc['describe'] == 'DEPLOY':
                set_tfjob_state(name, 0, tfjob.RUN)
                doc['describe'] = 'RUN'
                doc['cause_code'] = 'Running'
            elif doc['describe'] == 'ERROR':
                doc['cause_code'] = {"message": resp.status.conditions[0].message,
                                     "reason": resp.status.conditions[0].reason}
                try:
                    if resp.status.conditions[1]:
                        doc['cause_code'] = {"message": resp.status.conditions[1].message,
                                             "reason": resp.status.conditions[1].reason}
                except ConnectionClosed as e:
                    raise e
                except Exception as e:
                    print('resp.status.conditions[1] not exist')
                # doc['cause_code'] = {"message":resp.status.conditions[1].message, "reason":resp.status.conditions[1].reason}
        elif resp.status.phase == 'Pending':
            doc['describe'] = 'Pending'
            doc['cause_code'] = {"message": resp.status.conditions[0].message,
                                 "reason": resp.status.conditions[0].reason}
            try:
                if resp.status.conditions[1]:
                    doc['cause_code'] = {"message": resp.status.conditions[1].message,
                                         "reason": resp.status.conditions[1].reason}
            except ConnectionClosed as e:
                raise e
            except Exception as e:
                print('resp.status.conditions[1] not exist')
            # doc['cause_code'] = {"message":resp.status.conditions[1].message, "reason":resp.status.conditions[1].reason}
        elif resp.status.phase == 'Failed' or resp.status.phase =='Unknown':
            doc['cause_code'] = {"message": resp.status.conditions[0].message,
                                 "reason": resp.status.conditions[0].reason}
            try:
                if resp.status.conditions[1]:
                    doc['cause_code'] = {"message": resp.status.conditions[1].message,
                                         "reason": resp.status.conditions[1].reason}
            except ConnectionClosed as e:
                raise e
            except Exception as e:
                print('resp.status.conditions[1] not exist')
            # doc['cause_code'] = {"message":resp.status.conditions[1].message, "reason":resp.status.conditions[1].reason}
    return doc

def get_container_info(id):
    name = 'tf-container-' + id
    api = get_api()
    resp = None
    resp2 = None
    doc = {"container_id": name, "describe": 'ERROR', "cause_code": '',"host_ip":'', "pod_ip":'',"node_name":'',
           'host_port':'','host_port1':''}
    try:
        resp = api.read_namespaced_pod(name=name, namespace='default')
        print('resp', resp)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        print("Api Exception error: %s" % e,e)
        if e.status != 404:
            print("Unknown error: %s" % e)
        else:
            doc['cause_code'] = 'NOT-EXIST'

    except Exception as e:
        print('Exception', e)

    port = None
    port1 = None
    try:
        resp2 = api.read_namespaced_service(name=name,
                                    namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    if(resp2 != None):
        port = resp2.spec.ports[0].node_port
        print('svc port', port)
        print(len(resp2.spec.ports))
        if len(resp2.spec.ports)>1:
            port1 = resp2.spec.ports[1].node_port

    if resp:
        if resp.status.phase == 'Succeeded' or resp.status.phase == 'Running':
             doc['describe']= 'RUN'
             doc['pod_ip'] = resp.status.pod_ip
             doc['host_ip'] = resp.status.host_ip
             doc['node_name'] = resp.spec.node_name
             if port:
                 doc['host_port']=port
             if port1:
                 doc['host_port1'] = port1
        elif resp.status.phase == 'Pending':
            doc['describe'] = 'Pending'
            # doc['cause_code'] = 'Out of Resource'
            doc['cause_code'] = {"message": resp.status.conditions[0].message,
                                 "reason": resp.status.conditions[0].reason}
            try:
                if resp.status.conditions[1]:
                    doc['cause_code'] = {"message": resp.status.conditions[1].message,
                                         "reason": resp.status.conditions[1].reason}
            except ConnectionClosed as e:
                raise e
            except Exception as e:
                print('resp.status.conditions[1] not exist')
        elif resp.status.phase == 'Failed' or resp.status.phase == 'Unknown':
            doc['describe'] = 'ERROR'
            # doc['cause_code'] = 'PodError'
            doc['cause_code'] = {"message": resp.status.conditions[0].message,
                                 "reason": resp.status.conditions[0].reason}
            try:
                if resp.status.conditions[1]:
                    doc['cause_code'] = {"message": resp.status.conditions[1].message,
                                         "reason": resp.status.conditions[1].reason}
            except ConnectionClosed as e:
                raise e
            except Exception as e:
                print('resp.status.conditions[1] not exist')
        else:
            doc['describe'] = 'NOT-READY'
    print('doc',doc)
    return doc

def do_start_serving(id, model_name, model_dir, image=None, replicas=1, limits=None, home_dir='/data'):
    #api = get_api('AppsV1beta1Api')
    api = get_api('ExtensionsV1beta1Api')
    name = 'tensorflow-serving-' + id
    if image is None or image == '':
        image = app_config['serving_image']
    #model_name = 'mnist'
    port = 9000
    # command = ['/bin/sh', '-c', 'serving/bazel-bin/tensorflow_serving/model_servers/tensorflow_model_server'
    #            ' --port=' + str(port) + ' --model_name=' + model_name + ' --model_base_path=/data' + model_dir]
    command = ['/bin/sh', '-c', 'tensorflow_model_server'
               ' --port=' + str(port) + ' --model_name=' + model_name + ' --model_base_path=/data' + model_dir]

    args = None
    print(command)
    envs = None
    work_dir=None
    resp = deploy_create(api, name, image, replicas, command, args, port,
               work_dir, envs, limits, home_dir)
    print(resp)
    if resp is None:
        return False,None

    api = get_api()
    svc = service_create(api, name, port)
    print(svc)
    if svc is None:
        api = get_api('ExtensionsV1beta1Api')
        deploy_delete(api, name)
        return False,None
    port = svc.spec.ports[0].node_port
    print('svc port', port)
    #TODO write port info to mongodb
    #list pod
    try:
        pod = api.list_namespaced_pod(namespace='default',label_selector='app='+name)
        pod_name = pod.items[0].metadata.name
        print('list pod info',pod)
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('list pod error')
        return False,None
    return True, pod_name

def do_stop_serving(id):
    name = 'tensorflow-serving-' + id
    api = get_api('ExtensionsV1beta1Api')
    #api = get_api()

    resp = deploy_delete(api, name)
    api = get_api()
    resp = service_delete(api, name)
    if resp is None:
        return False
    else:
        return True

def wait_for_servering_ready( host, port, timeout=5):
    """Waits for a server on the localhost to become ready."""
    for _ in range(0, timeout):
        request = predict_pb2.PredictRequest()
        request.model_spec.name = 'intentionally_missing_model'

        try:
            # Send empty request to missing model
            channel = implementations.insecure_channel(host, port)
            stub = prediction_service_pb2.beta_create_PredictionService_stub(channel)
            stub.Predict(request, 2)
        except face.AbortionError as error:
            # Missing model error will have details containing 'Servable'
            print('get servable', error.details)
            if 'Servable' in error.details:
                print('Server is ready')
                return True
        time.sleep(1)
    return False

def get_serving_info(id):
    name = 'tensorflow-serving-' + id
    api = get_api()
    resp=None
    try:
        resp = api.read_namespaced_service(name=name,
                                    namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    if(resp == None):
        return None

    #print(resp)
    port = resp.spec.ports[0].node_port

    #api = get_api('ExtensionsV1beta1Api')
    host = None
    try:
        pods = api.list_namespaced_pod('default', label_selector='app='+name)
        for pod in pods.items:
            #host = pod.spec.node_name + '.ai'
            host = pod.status.host_ip
            break
            #print('pod', pod)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    print('host', host, 'port', port)

    if host is None:
        return None
    if wait_for_servering_ready(host, port):
        return {'name': name, 'host': host, 'port': port}
    return None


def get_serving_info_2(id):
    name = 'tensorflow-serving-' + id
    api = get_api('ExtensionsV1beta1Api')
    resp = None
    doc = {'name': name, 'describe':'ERROR' , 'cause_code':''}
    try:
        resp = api.read_namespaced_deployment(name=name,
                                              namespace='default')

    except ApiException as e:
        print("Api Exception error: %s" % e)
        if e.status != 404:
            print("Unknown error: %s" % e)
        else:
            doc['cause_code'] = 'NOT-EXIST'
    except ConnectionClosed as e:
        raise e
    except Exception as e:
        print('Exception',e)

    if resp:
        if resp.status.ready_replicas > 0:
            doc['describe'] = 'STARTING'
            if get_serving_info(id):
                doc['describe'] = 'RUN'
        else:
            doc['describe'] = 'NOT-READY'

    return doc



def do_start_cluster_tfjob(id, num_ps, num_worker,
                home_dir, work_dir, script_main, data_dir,
                model_dir, retrain,  epoch=1, learning_rate=0.01, momentum=0.01, decay_rate=0.01, batch_size=100, params=None, tensorboard=False,
                image=None, envs=None, limits_input=None):
    api = get_api()
    port = 2222
    resp = None
    #worker_hosts = ['tensorflow-job-cluster-'+id+'-worker-'+str(i)+':'+str(port) for i in range(num_worker)]
    #ps_hosts = ['tensorflow-job-cluster-'+id+'-ps-'+str(i)+':'+str(port) for i in range(num_ps)]
    worker_hosts = []
    ps_hosts = []
    job_num = [num_ps, num_worker]
    cluster_num = num_ps + num_worker
    job_name = ['ps', 'worker']
    hosts_list = [ps_hosts, worker_hosts]
    for i in range(len(job_name)):
        for j in range(job_num[i]):
            tfjob_name = 'tensorflow-job-cluster-%s-%s-%d' % (id, job_name[i], j)
            resp = service_create(api, tfjob_name, port)
            if resp is None:
                do_stop_cluster_tfjob(id)
                return False, None
            node_port = resp.spec.ports[0].port
            hosts_list[i].append('%s:%d' % (tfjob_name, node_port))
            body = {
                'metadata': {
                    'labels': {
                        'app': tfjob_name,
                        'tfcluster': 'cluster-' + id
                    }
                }
            }
            resp = api.patch_namespaced_service(tfjob_name, 'default', body)
            if resp is None:
                service_delete(api, tfjob_name)
                do_stop_cluster_tfjob(id)
                return False, None
            print('set cluster service label', resp)

    for i in range(len(job_name)):
        if i > 0:
            limits = limits_input['worker']
        else:
            limits = limits_input['ps']
        for j in range(job_num[i]):
            if params is None:
                job_params = ''
            else:
                job_params = params
            job_params = job_params + ' --job_name=' + job_name[i]
            job_params = job_params + ' --task_index=' + str(j)
            job_params = job_params + ' --ps_hosts=' + ','.join(ps_hosts)
            job_params = job_params + ' --worker_hosts=' + ','.join(worker_hosts)
            print(job_params)
            tfjob_name = 'cluster-%s-%s-%d' % (id, job_name[i], j)
            resp = do_start_tfjob_train(tfjob_name, home_dir, work_dir,
                        script_main, data_dir,
                        model_dir, retrain, epoch, learning_rate, momentum, decay_rate, batch_size, job_params, cluster_num=cluster_num, tensorboard=False,
                        image=image, envs=envs, limits=limits, port=port)
            print('start cluster tfjob', resp)
            if resp is None:
                do_stop_cluster_tfjob(id)
                return False, None
            body = {
                'metadata': {
                    'labels': {
                        'app': 'tensorflow-job-' + tfjob_name,
                        'tfcluster': 'cluster-' + id
                    }
                }
            }
            api = get_api()
            resp = api.patch_namespaced_pod('tensorflow-job-'+tfjob_name, 'default', body)
            if resp is None:
                do_stop_tfjob(tfjob_name)
                do_stop_cluster_tfjob(id)
                return False, None
            print('set cluster tfjob label', resp)

    url = None
    if tensorboard:
        url = do_start_tensorboard(id, home_dir, model_dir, limits)
        if url is None:
            do_stop_tfjob(id)
            return False, None
    return True, url

# def delete_collection_namespaced_pod2('default', label_selector=selector):
#     list xxx
#     del
def do_stop_cluster_tfjob(id):
    selector = 'tfcluster=cluster-' + id
    api = get_api()
    resp = api.delete_collection_namespaced_pod('default', label_selector=selector)
    # resp = api.get_coll
    print('delete pod', resp)
    # if flag is True:
    # do_stop_tensorboard(id)
    svcs = api.list_namespaced_service('default', label_selector=selector)
    for svc in svcs.items:
        print('svc', svc)
        resp = api.delete_namespaced_service(svc.metadata.name, 'default')
        print('delete service', resp)
    if resp is None:
        return False
    else:
        return True

def do_start_deploy_job(name, image, work_dir, command, replicas=1,
                        limits=None, home_dir='/data', data_dir=None):
    #api = get_api('AppsV1beta1Api')
    api = get_api('ExtensionsV1beta1Api')
    port = 9000
    args = None
    print(command)
    envs= []
    envs.append({'name': 'MONGODB_HOST', 'value': app_config['mongodb_host']})
    envs.append({'name': 'MONGODB_PORT', 'value': app_config['mongodb_port']})
    resp = deploy_create(api, name, image, replicas, command, args, port,
               work_dir, envs, limits, home_dir, data_dir)
    print(resp)
    if resp is None:
        return False

    #TODO write port info to mongodb
    return True

def do_stop_deploy_job(name):
    api = get_api('ExtensionsV1beta1Api')
    resp = deploy_delete(api, name)
    if resp is None:
        return False
    else:
        return True

def do_get_container_pod(name):
    api = get_api()
    resp1 = None
    resp2 = None
    try:
        resp1 = api.read_namespaced_pod_status(name=name,
                                           namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    try:
        resp2 = api.read_node_status(name=name,
                                    namespace='default')
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)


def optimization_advice(train_accurate_rate, test_accurate_rate):
    if train_accurate_rate < 0.8:
        return 'underfitting', ['Add neural network layers', 'Add train epochs', 'Reduce learning rate']
    elif test_accurate_rate < train_accurate_rate * 0.8:
        return 'overfitting', ['Add dropout layer for neural network', 'Reduce train epochs']
    else:
        return 'ok', []


def get_gpu_statis(pod_name):
    '''
    get pod gpu statis
    Args:
        pod_name: pod name to query
    Returns:
        statis: an array of dict, eatch dict for a gpu statis, dict has follow attribute
            'mem_total': int type gup memeory totoal in MBi
            'mem_used': int type gpu memory used in MBi
            'gpu_usage': int type gpu usage in percent
            'temp_cur': int type current temperature of gpu in centigrade degree
            'temp_limit': int type temperature of gpu limit in centigrade degree
            'power_cur': float type current power of gpu in watt
            'power_limit': float type power of gpu limit in watt
    '''
    # Configs can be set in Configuration class directly or using helper utility
    api = get_api()

    # calling exec and wait for response.
    statis = []
    for i in range(8):
        exec_command = [
			'nvidia-smi',
			'-i',
			str(i),
			'-q',
			'-d',
			'UTILIZATION,MEMORY,TEMPERATURE,POWER']
        try:
            resp = pod_exec(api, pod_name, exec_command)
        except Exception as e:
            print('error-gpu',e)
        if resp :
            print("Response: " + resp)
            mem_total = re.findall(r"Total\s*: (.+?)\s*MiB", resp)
            mem_used = re.findall(r"Used\s*: (.+?)\s*MiB", resp)
            gpu_usage = re.findall(r"Gpu\s*: (.+?)\s*%", resp)
            temp_cur = re.findall(r"GPU Current Temp\s*: (.+?)\s*C", resp)
            temp_limit = re.findall(r"GPU Shutdown Temp\s*: (.+?)\s*C", resp)
            power_cur = re.findall(r"Power Draw\s*: (.+?)\s*W", resp)
            power_limit = re.findall(r"Power Limit\s*: (.+?)\s*W", resp)
            if mem_total and len(mem_total) > 0:
                statis.append({'mem_total':int(mem_total[0]),
                               'mem_used':int(mem_used[0]),
                               'gpu_usage':int(gpu_usage[0]),
                               'temp_cur':int(temp_cur[0]),
                               'temp_limit':int(temp_limit[0]),
                               'power_cur':float(power_cur[0]),
                               'power_limit':float(power_limit[0])
                               })
                #print('statis:', mem_total[0], mem_used[0], gpu_usage[0])
                continue

        break
    print('statis', statis)
    return statis

def get_service_port(name, namespace='default'):
    api = get_api()
    resp=None
    try:
        resp = api.read_namespaced_service(name=name,
                                    namespace=namespace)
    except ConnectionClosed as e:
        raise e
    except ApiException as e:
        if e.status != 404:
            print("Unknown error: %s" % e)

    if(resp == None):
        return None

    #print(resp)
    port = resp.spec.ports[0].node_port
    return port

def get_node_statis(node_name):
    '''
    Result: ResultSet({'(u'cpu/temperature', None)': [{u'last': u'33.0,30.0',
        u'nodename': u'uis-1.ai', u'time': u'2018-04-26T12:36:33.872781021Z'}]})
    Result: ResultSet({'(u'gpu/info', None)': [{u'gpu_name': u'GeForce GTX 1080 Ti',
        u'power_limit': 250, u'last': 0, u'nodename': u'uis-1.ai', u'resource_id': 0,
        u'gpu_usage': 0, u'cluster_name': u'default', u'mem_used': 0, u'mem_total': 11172,
        u'time': u'2018-04-26T12:36:33.864488036Z', u'temp_limit': 96, u'power_cur': 16.19,
        u'type': u'node', u'temp_cur': 31}]})
    ('statis', {'gpu': [{u'gpu_name': u'GeForce GTX 1080 Ti', u'gpu_usage': 0,
        u'power_limit': 250, 'mem_used': 0, u'mem_total': 11172,
        u'time': u'2018-04-26T16:33:31.656211409Z', u'temp_limit': 96, u'power_cur': 16.1,
        u'temp_cur': 30}], 'cpu': {'temp': u'32.0,29.0', 'time': u'2018-04-26T16:33:31.664546948Z'}})

    '''
    #client = InfluxDBClient('10.1.2.4', 8086, 'root', 'root', 'k8s')
    port = get_service_port('monitoring-influxdb', 'kube-system')
    host = app_config['k8s_master_host']
    print('influxdb info', host, port)
    client = InfluxDBClient(host, port, 'root', 'root', 'k8splus')
    #print('measurement', client.get_list_measurements())
    statis = {}
    result = client.query('select last(value), time, nodename from "cpu/temperature" '
                          'where nodename=\'%s\' and time > now()-2m;' % node_name)
    points = list(result.get_points())
    if len(points)>0:
        point = points[-1]
        statis['cpu'] = {'temp': point['last'], 'time': point['time']}
    #print("Result: {0}".format(points))
    gpu = []
    for id in range(8):
        result = client.query('select last(mem_used), mem_total, power_cur, power_limit, gpu_usage, temp_cur, '
                            'temp_limit, gpu_name from "gpu/info" '
                            'where nodename=\'%s\' and resource_id=%d and time > now()-2m;'
                            % (node_name, id))
        points = list(result.get_points())
        if len(points)>0:
            point = points[-1]
            point['mem_used'] = point['last']
            del point['last']
            gpu.append(point)
    if len(gpu)>0:
        statis['gpu'] = gpu
    print('statis', statis)
    return statis

# def do_delete_node(node_list):
#     api = get_api()
#     result = []
#     resp = None
#     print('node_list',node_list)
#     for node in node_list:
#         print('node',node)
#         # body = {
#         #     'apiVersion': 'V1',
#         #     'kind': 'Node',
#         #     'metadata': {
#         #         'name': node,
#         #         'labels': {
#         #             'kubernetes.io/hostname': node
#         #         }
#         #     },
#         #     'spec': {
#         #         'unschedulable': 'true'
#         #     }
#         # }
#         body = client.V1DeleteOptions(grace_period_seconds=1)
#         try:
#             resp = api.delete_node(name=node, body=body)
#         except Exception as e:
#             current_result = {'node_name': node, 'success': False, 'reason': e.reason}
#             result.append(current_result)
#             print('delete_node_error',e)
#         if resp is None:
#             print('delete_node_info', resp)
#         else:
#             print('delete_node_info', resp)
#             current_result = {'node_name': node, 'success': True, 'reason': ""}
#             result.append(current_result)
#
#     print('delete_node_result', result)
#     return result
#
# def do_create_node(node_list):
#     api = get_api()
#     result = []
#     resp = None
#     print('node_list', node_list)
#     for node_name in node_list:
#         print('node_name', node_name)
#         body = {
#             'kind': 'Node',
#             'apiVersion': 'v1',
#             'metadata': {
#                 'name': node_name
#             }
#         }
#         try:
#             resp = api.create_node(body)
#         except Exception as e:
#             current_result = {'node_name': node_name, 'success': False, 'reason': e.reason}
#             result.append(current_result)
#             print('create_node_error', e.reason)
#         if resp is None:
#             print('create_node_info', resp)
#         else:
#             print('create_node_info', resp)
#             current_result = {'node_name': node_name, 'success': True, 'reason': ""}
#             result.append(current_result)
#
#     print('create_node_result', result)
#     return result

def do_patch_node_schedulable(node_list):
    api = get_api()
    result = []
    resp = None
    print('node_list', node_list)
    for node_name in node_list:
        print('node_name', node_name)
        body = {
            "spec": {
                "unschedulable": False
            }
        }
        try:
            resp = api.patch_node(node_name, body)
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            current_result = {'node_name': node_name, 'success': False, 'reason': e.reason}
            result.append(current_result)
            print('patch_node_schedulable_error', e.reason)
        if resp is None:
            print('patch_node_schedulable_info', resp)
        else:
            print('patch_node_schedulable_info', resp)
            current_result = {'node_name': node_name, 'success': True, 'reason': ""}
            result.append(current_result)

    print('patch_node_schedulable_result', result)
    return result


def do_patch_node_unschedulable(node_list):
    api = get_api()
    result = []
    resp = None
    print('node_list', node_list)
    for node_name in node_list:
        print('node_name', node_name)
        body = {
            "spec": {
                "unschedulable": True
            }
        }
        try:
            resp = api.patch_node(node_name, body)
        except ConnectionClosed as e:
            raise e
        except Exception as e:
            current_result = {'node_name': node_name, 'success': False, 'reason': e.reason}
            result.append(current_result)
            print('patch_node_unschedulable_error', e.reason)
        if resp is None:
            print('patch_node_unschedulable_info', resp)
        else:
            print('patch_node_unschedulable_info', resp)
            current_result = {'node_name': node_name, 'success': True, 'reason': ""}
            result.append(current_result)

    print('patch_node_unschedulable_result', result)
    return result




