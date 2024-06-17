from yaml import dump

class ConfigError(Exception):
    pass

class BenchParameters:
    def __init__(self, data):
        try:
            nodes = data['nodes'] 
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 0 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')
            
            batch_size = data['batch_size'] 
            batch_size = batch_size if isinstance(batch_size, list) else [batch_size]
            if not batch_size:
                raise ConfigError('Missing batch_size')

            self.nodes = [int(x) for x in nodes]
            self.log_level = int(data['log_level'])
            self.batch_size = [int(x) for x in batch_size]
            self.duration = int(data['duration'])
            self.round = int(data['round'])
            self.runs = int(data['runs']) if 'runs' in data else 1
            self.node_instance = int(data['node_instance']) if 'node_instance' in data else 1
            self.fault_number = int(data["faulty_number"])
            self.protocol = data['protocol']
            self.yaml = data
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')
        
    def update_addr(self,ids,ips,ports):
        mp1,mp2 = {},{}
        for (node,ip,port) in zip(ids,ips,ports):
            mp1[node] = ip
            mp2[node] = port
        self.yaml["IPs"] = mp1
        self.yaml["peers_p2p_port"] = mp2

    def print(self,batch_size,filename):
        self.yaml["batch_size"] = batch_size
        with open(filename,"a") as f:
            dump(self.yaml,f)
