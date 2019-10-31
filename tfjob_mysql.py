
import os
import re
import pymysql
import time
import subprocess
import sys


def get_ip_addr():
    ip_addr = os.getenv("RDMA_IP", "10.99.0.1")
    ip_process = ip_addr.split(".", 2)
    ip_process = ip_process[0] + '.' + ip_process[1]
    return ip_process


def set_rdma_device():
    try:
        res = run_cmd('ibdev2netdev')
        print('run ibdev2netdev:', res)
        pos = res.find(' ')
        if pos >= 1:
            name = res[0:pos]
            print('find ib device:', name)
            subprocess.call("echo 'export RDMA_DEVICE={}' >> /etc/profile".format(name), shell=True)
    except:
        print('this images does not support this command "ibdev2netdev"')


def run_cmd(cmd):
    rs = os.popen(cmd)
    cmdout = rs.read()
    rs.close()
    print('out', cmdout)
    return cmdout


def get_container_ip():
    f_ip = os.popen("hostname -I")
    f_hostname = os.popen("hostname")
    ip_list = f_ip.read()
    host_name = str(f_hostname.read())
    ip_list = filter(None, ip_list.split(" "))
    if '\n' in ip_list:
        ip_list.remove('\n')
    if '\n' in host_name:
        host_name = host_name.strip('\n')
    print('ip_list', ip_list)
    print('host_name', host_name)
    return host_name, ip_list


def rdma_ip_to_sql(host, user, passwd, port):
    rdma_ip = {"hostname": "", "rdma_ip": ""}
    hostname, container_ip = get_container_ip()
    rdma_ip["hostname"] = hostname
    ip_process = get_ip_addr()
    for i in range(len(container_ip)):
        resp = re.match(ip_process, container_ip[i])
        if resp:
            rdma_ip["rdma_ip"] = container_ip[i]
            break
    conn = pymysql.connect(host=host, user=user, passwd=passwd, port=port, db="aios")
    try:
        with conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            sql = 'CREATE TABLE IF NOT EXISTS rd_ip(hostname VARCHAR(255) NOT NULL, rdma_ip VARCHAR(255) NOT NULL)'
            cursor.execute(sql)
            cols = ", ".join('`{}`'.format(k) for k in rdma_ip.keys())
            val_cols = ', '.join('%({})s'.format(k) for k in rdma_ip.keys())
            sql = "insert into rd_ip(%s) values(%s)"
            res_sql = sql % (cols, val_cols)
            cursor.execute(res_sql, rdma_ip)
            conn.commit()
    except Exception as e:
        print("mysql error", e)
        raise e
    finally:

        conn.close()


def get_rdma_ip(host_name, host, user, passwd, port):
    resp = {"hostname": host_name, "rdma_ip": ""}
    conn = pymysql.connect(host=host, user=user, passwd=passwd, port=port, db="aios")
    try:
        with conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            sql = 'SELECT hostname, rdma_ip FROM rd_ip WHERE hostname="{}"'.format(host_name)
            cursor.execute(sql)
            doc = cursor.fetchall()
            if len(doc) == 0:
                return resp
            resp["rdma_ip"] = doc[0]["rdma_ip"].encode('utf-8')
    except Exception as e:
        print("get ip from mysql error", e)
        raise e
    finally:
        conn.close()
    print("get rdma ip", resp)
    return resp


def switch_to_ip(host, user, passwd, port):
    ps_spec_new = []
    worker_spec_new = []
    if os.environ['JOB_HOST_LIST_ps'] is None or os.environ['JOB_HOST_LIST_worker'] is None:
        print("JOB_HOST_LIST_ps is None or JOB_HOST_LIST_worker is None")
        return
    ps_spec = os.environ['JOB_HOST_LIST_ps']
    worker_spec = os.environ['JOB_HOST_LIST_worker']
    ps_spec = ps_spec.split(",")
    worker_spec = worker_spec.split(",")
    try:
        set_rdma_device()
        rdma_ip_to_sql(host=host, user=user, passwd=passwd, port=port)
        time_begin_ps = time.time()
        timeout_ps = 300*len(ps_spec)
        timeout_worker = 300*len(worker_spec)
        for host_port in ps_spec:
            while True:
                host_rd = host_port.split(":")[0]
                port_rd = host_port.split(":")[1]
                resp = get_rdma_ip(host_rd, host, user, passwd, port)
                if resp["rdma_ip"] is not "":
                    ps_spec_new.append(resp["rdma_ip"]+":"+port_rd)
                    print('ps rdma_ip is found')
                    break
                else:
                    print("continue to find the ps_ip in db")
                    time.sleep(2)
                    if (time.time() - time_begin_ps) > timeout_ps:
                        print("the default maximum timeout")
                        return False
                    continue
        time_begin_worker = time.time()
        for host_port in worker_spec:
            while True:
                host_rd = host_port.split(":")[0]
                port_rd = host_port.split(":")[1]
                resp = get_rdma_ip(host_rd, host, user, passwd, port)
                if resp["rdma_ip"] is not "":
                    worker_spec_new.append(resp["rdma_ip"]+":"+port_rd)
                    print('worker rdma_ip is found')
                    break
                else:
                    print("continue to find the worker_ip in db")
                    time.sleep(1)
                    if (time.time() - time_begin_worker) > timeout_worker:
                        print("the default maximum timeout")
                        return False
                    continue
        if len(ps_spec_new) == 1:
            ps_spec_new = "".join(ps_spec_new)
        elif len(ps_spec_new) > 1:
            ps_spec_new = ",".join(ps_spec_new)
        if len(worker_spec_new) == 1:
            worker_spec_new = "".join(worker_spec_new)
        elif len(worker_spec_new) > 1:
            worker_spec_new = ",".join(worker_spec_new)
        print("ps_spec_new is {},worker_spec_new is {}".format(ps_spec_new, worker_spec_new))
        subprocess.call("echo 'JOB_HOST_LIST_ps='{}'' >> /etc/profile".format(ps_spec_new), shell=True)
        subprocess.call("echo 'JOB_HOST_LIST_worker='{}'' >> /etc/profile".format(worker_spec_new), shell=True)

    except Exception as e:
        print('wait failed', e)
    return False


if __name__ == '__main__':
    try:
        if len(sys.argv) == 5:
            switch_to_ip(host=sys.argv[1], user=sys.argv[2], passwd=sys.argv[3],
                         port=int(sys.argv[4]))
    except Exception as e:
        print("command exec-error", e)

