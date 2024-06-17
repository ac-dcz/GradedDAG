import subprocess
from math import ceil
from os.path import basename, join, splitext
from time import sleep

from benchmark.config import BenchParameters,ConfigError
from benchmark.commands import CommandMaker
from benchmark.logs import ParseError
from benchmark.utils import Print, BenchError, PathMaker
from datetime import datetime

class LocalBench:
    BASE_PORT = 6000

    def __init__(self, bench_parameters_dict):
        try:
            self.ts = datetime.now().strftime("%Y-%m-%dv%H:%M:%S")
            self.bench_parameters = BenchParameters(bench_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)


    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, batch_size = self.bench_parameters.nodes[0],self.bench_parameters.batch_size[0]
            # Cleanup all files.
            cmd = f'{CommandMaker.cleanup_configs()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5) # Removing the store may take time.
            
            # make logs
            cmd = f'{CommandMaker.make_logs_dir(self.ts)}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True)

            # Generate configuration files.
            ids,ips,ports = [],[],[]
            for i in range(nodes):
                ids.append(f'node{i}')
                ips.append('127.0.0.1')
                ports.append(self.BASE_PORT+i*100)
            self.bench_parameters.update_addr(ids,ips,ports)
            self.bench_parameters.print(batch_size,PathMaker.config_template_file())
            cmd = CommandMaker.run_config().split()
            subprocess.run(cmd, check=True)

            Print.info(f'Running {self.bench_parameters.protocol}')
            Print.info(f'{self.bench_parameters.fault_number} byzantine nodes')
            Print.info(f'batch_size {batch_size}')

            # Run the nodes.
            for i in range(nodes):
                for j in range(self.bench_parameters.node_instance):
                    cmd = CommandMaker.run_node(PathMaker.node_config_file(i,j))
                    self._background_run(cmd, PathMaker.node_log_info_file(i*self.bench_parameters.node_instance+j,self.ts))

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(15)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.bench_parameters.duration} sec)...')
            sleep(self.bench_parameters.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
