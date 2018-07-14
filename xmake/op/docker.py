import io
import logging
import os
import tarfile
import threading
from _signal import SIGTERM
from datetime import datetime
from os.path import expanduser
from typing import Any, List, Dict, Tuple, Optional, Union
from urllib.parse import ParseResult, urlparse

from dataclasses import field, dataclass
from docker import DockerClient
from docker.constants import DEFAULT_DOCKER_API_VERSION
from docker.types import ContainerConfig as _CC, HostConfig as _HC

from xmake.dsl import Op, Var, TRes


class Obj(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.update({'_type': self.__class__.__name__})

    def __getattr__(self, name):

        if name in self:
            return self[name]
        else:
            name = name.title()

            if name in self:
                return self[name]
            else:
                logging.getLogger(__name__).debug('%s', name)
                raise AttributeError(name)

    @classmethod
    def m(cls, x):
        return x.get('_type') == cls.__name__


class Image(Obj):
    @classmethod
    def tag(cls, tag):
        tag = tag.split(':')

        if len(tag) == 1:
            tag.append('latest')

        r = ':'.join(tag)

        return r


class Exec(Obj):
    pass


class Network(Obj):
    pass


class Container(Obj):
    pass


@dataclass()
class Docker(Op):
    conf: Op = Var('docker')

    def dependencies(self) -> List['Op']:
        return [self.conf]

    def execute(self, arg: DockerClient):
        return DockerClient(arg)


class DockerOp(Op):
    def dependencies(self) -> List[Op]:
        return [Docker()]

    def execute(self, c: DockerClient):
        raise NotImplementedError('')


class Mappable:
    def get(self, version) -> Any:
        raise NotImplementedError()


@dataclass(init=False)
class HostConfig(Mappable):
    args: List[Any]

    def __init__(self, binds=None, port_bindings=None, lxc_conf=None, publish_all_ports=False, links=None,
                 privileged=False, dns=None, dns_search=None, volumes_from=None, network_mode=None, restart_policy=None,
                 cap_add=None, cap_drop=None, devices=None, extra_hosts=None, read_only=None, pid_mode=None,
                 ipc_mode=None, security_opt=None, ulimits=None, log_config=None, mem_limit=None, memswap_limit=None,
                 mem_reservation=None, kernel_memory=None, mem_swappiness=None, cgroup_parent=None, group_add=None,
                 cpu_quota=None, cpu_period=None, blkio_weight=None, blkio_weight_device=None, device_read_bps=None,
                 device_write_bps=None, device_read_iops=None, device_write_iops=None, oom_kill_disable=False,
                 shm_size=None, sysctls=None, tmpfs=None, oom_score_adj=None, dns_opt=None, cpu_shares=None,
                 cpuset_cpus=None, userns_mode=None, pids_limit=None, isolation=None, auto_remove=False,
                 storage_opt=None, init=None, init_path=None, volume_driver=None, cpu_count=None, cpu_percent=None,
                 nano_cpus=None, cpuset_mems=None, runtime=None, mounts=None, cpu_rt_period=None, cpu_rt_runtime=None,
                 device_cgroup_rules=None):
        self.args = (binds, port_bindings, lxc_conf, publish_all_ports, links, privileged, dns, dns_search,
                     volumes_from, network_mode, restart_policy, cap_add, cap_drop, devices, extra_hosts, read_only,
                     pid_mode, ipc_mode, security_opt, ulimits, log_config, mem_limit, memswap_limit,
                     mem_reservation, kernel_memory, mem_swappiness, cgroup_parent, group_add, cpu_quota,
                     cpu_period, blkio_weight, blkio_weight_device, device_read_bps, device_write_bps,
                     device_read_iops, device_write_iops, oom_kill_disable, shm_size, sysctls, tmpfs, oom_score_adj,
                     dns_opt, cpu_shares, cpuset_cpus, userns_mode, pids_limit, isolation, auto_remove, storage_opt,
                     init, init_path, volume_driver, cpu_count, cpu_percent, nano_cpus, cpuset_mems, runtime,
                     mounts, cpu_rt_period, cpu_rt_runtime, device_cgroup_rules)

    def __repr__(self) -> str:
        return str(self.get(DEFAULT_DOCKER_API_VERSION))

    def get(self, version) -> _HC:
        return _HC(version, *self.args)


@dataclass(init=False)
class ContainerConfig(Mappable):
    args: List[Any]

    def __repr__(self) -> str:
        return str(self.get(DEFAULT_DOCKER_API_VERSION, '<UNKNOWN>', ['<UNKNOWN>']))

    def __init__(self, hostname=None, user=None, detach=False, stdin_open=False, tty=False,
                 ports=None, environment=None, volumes=None, network_disabled=False, entrypoint=None, working_dir=None,
                 domainname=None, host_config: HostConfig = HostConfig(), mac_address=None, labels=None,
                 stop_signal=None,
                 networking_config=None, healthcheck=None, stop_timeout=None, runtime=None):
        self.args = (hostname, user, detach, stdin_open, tty, ports, environment, volumes,
                     network_disabled, entrypoint, working_dir, domainname, host_config, mac_address, labels,
                     stop_signal, networking_config, healthcheck, stop_timeout, runtime)

    def get(self, version, image, command) -> _CC:
        return _CC(version, image, command, *(x.get(version) if isinstance(x, Mappable) else x for x in self.args))


ContainerPutFiles = Dict[str, Tuple[tarfile.TarInfo, bytes]]


@dataclass()
class ContainerPut(DockerOp):
    c: Container
    path: str
    files: ContainerPutFiles

    @classmethod
    def tarinfos(cls, items: Dict[str, str], *, modes=0o644, time: Optional[datetime] = None) -> ContainerPutFiles:
        if time is None:
            time = datetime.now()
        r = {}
        for path, contents in items.items():
            if isinstance(contents, str):
                contents = contents.encode()
            ti = tarfile.TarInfo(path)
            ti.mode = modes
            ti.size = len(contents)
            ti.mtime = int(time.timestamp())
            r[path] = (ti, contents)
        return r

    @classmethod
    def tarinfos_files(cls, items: Dict[str, str], *, modes=None, time: Optional[datetime] = None) -> ContainerPutFiles:
        r = {}
        for dest_path, path in items.items():
            path = expanduser(path)
            stat = os.stat(path, follow_symlinks=True)
            mtime = stat.st_mtime
            if time is not None:
                mtime = int(time.timestamp())
            mode = stat.st_mode
            if modes is not None:
                mode = modes

            with open(path, 'rb') as f_obj:
                contents = f_obj.read()

            ti = tarfile.TarInfo(dest_path)
            ti.mode = mode
            ti.size = len(contents)
            ti.mtime = mtime
            r[path] = (ti, contents)
        return r

    def execute(self, c: DockerClient):
        file_like_object = io.BytesIO()
        tar = tarfile.open(fileobj=file_like_object, mode='w')
        for path, (tf, contents) in self.files.items():
            if isinstance(contents, str):
                contents = contents.encode()

            fileobj = io.BytesIO(contents)
            tar.addfile(tf, fileobj)
            pass

        file_like_object.seek(0)

        return c.api.put_archive(self.c, self.path, file_like_object.getvalue())


@dataclass()
class DockerAuth:
    username: Optional[str] = field(default=None, repr=False)
    password: Optional[str] = field(default=None, repr=False)

    @classmethod
    def from_auths(cls, auths, id='bpmms'):
        url = auths['docker']['registry'][id]
        r: ParseResult = urlparse(url)
        return DockerAuth(r.username, r.password)


@dataclass()
class ImageList(DockerOp):
    name: str = None
    quiet: bool = False
    all: bool = True
    filters: Optional[Dict[str, str]] = None

    def execute(self, c: DockerClient):
        return [Image(x) for x in c.api.images(self.name, self.quiet, self.all, self.filters)]


@dataclass()
class ImagePull(DockerOp):
    tag: str
    auth: Optional[DockerAuth] = None

    def execute(self, c: DockerClient):
        repo, tag = Image.tag(self.tag).split(':')
        auth_config = None
        if self.auth:
            auth_config = {'username': self.auth.username, 'password': self.auth.password}

        stream = True

        r = c.api.pull(repo, tag, auth_config=auth_config, stream=stream, decode=True)

        if stream:
            for x in r:
                print(x)
                logging.getLogger(__name__ + f'.{self.__class__.__name__}').debug('%s', x)
        else:
            return r


@dataclass()
class ImageTag(DockerOp):
    i: Image
    tag: str

    def execute(self, c: DockerClient) -> TRes:
        repo, tag = Image.tag(self.tag)
        r = c.api.tag(self.i.id, repo, tag, )
        return r


@dataclass()
class ExecCreate(DockerOp):
    c: Container
    command: List[str]

    def execute(self, c: DockerClient):
        return Exec(c.api.exec_create(self.c['Id'], self.command))


@dataclass()
class ExecStart(DockerOp):
    e: Exec

    def execute(self, c: DockerClient):
        for pkt in c.api.exec_start(self.e.id, stream=True):
            try:
                pkt = pkt.decode()
            except:
                logging.exception('')
                pkt = str(pkt)

            pkts = pkt.split('\n')
            pkts = (y for y in pkts if len(y))
            for pkt in pkts:
                logging.getLogger(__name__ + f'.{self.__class__.__name__}').debug('%s', pkt)


@dataclass()
class ContainerList(DockerOp):
    quiet: bool = False
    all: bool = True
    filters: Optional[Dict[str, Union[List[str], str]]] = None

    def execute(self, c: DockerClient):
        return [Container(x) for x in c.api.containers(quiet=self.quiet, all=self.all, filters=self.filters)]


@dataclass()
class ContainerCreate(DockerOp):
    i: Image
    name: Optional[str] = None
    command: List[str] = field(default_factory=list)
    config: ContainerConfig = field(default_factory=ContainerConfig)

    def execute(self, c: DockerClient):
        cfg = self.config.get(c.api.api_version, self.i['Id'], self.command)

        r = c.api.create_container_from_config(
            cfg,
            name=self.name)

        return Container(r)


@dataclass()
class ContainerKill(DockerOp):
    c: Container
    signal: int = SIGTERM

    def execute(self, c: DockerClient):
        return c.api.kill(c['Id'], self.signal)


@dataclass()
class ContainerRemove(DockerOp):
    c: Container
    v = True
    link = False
    force = True

    def execute(self, c: DockerClient):
        r = c.api.remove_container(self.c.id, self.v, self.link, self.force)

        return r


@dataclass()
class ContainerStart(DockerOp):
    c: Container

    def execute(self, c: DockerClient):
        x1 = self.c.get('State')
        x2 = self.c.get('Id')
        assert x1 not in ['running'], f'Status is {x1}'
        assert x2, 'Must be hydrated'

        return c.api.start(self.c['Id'])


@dataclass()
class ContainerCommit(DockerOp):
    c: Container
    tag: Optional[str] = None
    message: Optional[str] = None
    author: Optional[str] = None
    changes: Optional[Union[str, List[str]]] = None
    conf: Optional[str] = None

    def execute(self, c: DockerClient):
        repo, tag = None, None

        if self.tag:
            repo, tag = Image.tag(self.tag).split(':')

        return Image(c.api.commit(self.c.id, repo, tag, self.message, self.author, self.changes, self.conf))


@dataclass()
class ContainerPause(DockerOp):
    c: Container

    def execute(self, c: DockerClient):
        x1 = self.c.get('State')
        x2 = self.c.get('Id')
        # assert x1 not in ['running'], f'Status is {x1}'
        # assert x2, 'Must be hydrated'

        return c.api.pause(self.c.id)


@dataclass()
class NetworkCreate(DockerOp):
    n: Network
    name: str
    driver: str = 'bridge'
    check_duplicate: bool = True
    labels: Optional[Dict[str, str]] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def execute(self, c: DockerClient):
        assert self.n.get('Id') is None, 'Must not be hydrated'

        ##labels = self.kwargs.get('labels', {})
        # labels = {**labels, **{self.conf.label: ''}}
        # kwargs = {**self.kwargs, 'labels': labels}
        kwargs = self.kwargs
        r = c.api.create_network(
            self.name, driver=self.driver,
            check_duplicate=self.check_duplicate,
            labels=self.labels,
            **kwargs)
        self.n.update(r)
        return r


@dataclass()
class ContainerNetworkAssign(DockerOp):
    c: Container
    n: Network
    aliases: List[str] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def execute(self, c: DockerClient):
        return c.api.connect_container_to_network(self.c['Id'], self.n['Id'], aliases=self.aliases, **self.kwargs)
