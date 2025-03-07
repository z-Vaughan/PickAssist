import os



class ResourceFinder:
    def __init__(self):
        pass
    @classmethod
    def find_resource(cls, resource):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        resource_path = os.path.join(script_dir, "res", resource)
        return resource_path