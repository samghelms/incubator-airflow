# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# TODO: should this operator take a whole directory so people
# don't have to worry about the location of csv files used in
# their ipython notebooks?

import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class IPythonOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param notebook_location: A url to the notebook. 
    :type bash_command: str
    :param env: If env is not None, it must be a url to an environment ...
    <TODO: decide whether this needs to be a conda-packed environment/think about options for docker>
    """
    template_fields = ('ipython_notebook',)
    template_ext = ('.ipynb',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            notebook_location,
            env=None,
            *args, **kwargs):

        super(IPythonOperator, self).__init__(*args, **kwargs)
        self.notebook_location = notebook_location
        self.env = env

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info("Tmp dir root location: \n %s", gettempdir())

        self.log.info("setting up python environment:\n")
        # TODO: Setup python environment.
        # if self.env is None:
        #     self.env = os.environ.copy()

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                # TODO: write ipynb from address to this file
                f.write(bytes('', 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )
                # TODO: add a warning message here for people who forget 
                # that paths for reading data are relative

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command: %s", self.bash_command)
                sp = Popen(
                    ['jupyter nbconvert --execute', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=pre_exec)

                self.sp = sp

                self.log.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)
                sp.wait()
                self.log.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
