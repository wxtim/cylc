from copy import deepcopy
import optparse


class InterfaceGenerator():

    @classmethod
    def generate(cls, mutation):
        state = {}
        interface = cls.visit(mutation, state)
        for argument in mutation['args']:
            cls.generate_node(argument, interface, state)
        depart = getattr(cls, 'depart', None)
        if depart:
            depart(mutation, interface, state)
        return interface

    @classmethod
    def generate_node(cls, argument, interface, state, depart=True):
        visit, depart_ = cls.get_route(argument)
        vis = visit(argument, interface, state)
        dep = None
        if depart and depart_:
            dep = depart_(argument, interface, state)
        return (vis, dep)

    @classmethod
    def get_route(cls, argument):
        if 'type' in argument:
            type_ = argument['type']
        else:
            type_ = argument['ofType']
        #name = (type_['name'] or type_['kind']).lower()
        for attr in ('name', 'kind'):
            type_id = type_[attr]
            if not type_id:
                continue
            type_id = type_id.lower()
            try:
                return (
                    getattr(cls, f'visit_{type_id}'),
                    getattr(cls, f'depart_{type_id}', None)
                )
            except AttributeError:
                continue
        else:
            print(f'Unsupported type "{type_["kind"]}", falling back to SCALAR.')
            new_argument = dict(argument)
            new_argument['name'] = None
            new_argument['kind'] = 'SCALAR'
            return cls.get_route(new_argument)

    @classmethod
    def visit(cls, mutation, state):
        raise NotImplementedError


class OptParseInterfaceGenerator(InterfaceGenerator):

    TYPE_MAP = {
        'String': str,
        'Int': int
    }

    @classmethod
    def visit(cls, mutation, state):
        parser = optparse.OptionParser()
        state['usage'] = {}
        return parser

    @classmethod
    def depart(cls, mutation, interface, state):
        usage = interface.usage
        if state['usage']:
            usage += ' ' + (' '.join(state['usage']))

        description = mutation['description']
        if state['usage']:
            description += '\n\nArguments:'
            description += '\n' + ('\n'.join(
                f'  {name}{" " * (20 - len(name))}{desc}'
                for name, desc in state['usage'].items()
            ))
        #interface.description = description

        interface.usage = usage + '\n\n' + description

    @classmethod
    def make_option(cls, argument, interface, state):
        print('%', state)
        interface.add_option(
            *state['args'],
            **state['kwargs']
        )
        #state['args'] = tuple()
        #state['kwargs'] = dict()

    @classmethod
    def visit_non_null(cls, argument, interface, state):
        state['usage'][argument['name']] = argument['description']

    #@classmethod
    #def visit_string(cls, argument, interface, state):
    #    return cls.visit_scalar(argument, interface, state)

    @classmethod
    def visit_scalar(cls, argument, interface, state):
        name = f'--{argument["name"]}'
        return interface.add_option(
            name,
            help=argument['description'],
            type=cls.TYPE_MAP.get(argument['type']['name'], str)
        )

    @classmethod
    def visit_scalar(cls, argument, interface, state):
        state['args'] = (
            f'--{argument["name"]}',
        )
        state['kwargs'] = {
            'help': argument['description'],
            'type': cls.TYPE_MAP.get(argument['type']['name'], str)
        }

    depart_scalar = make_option

    @classmethod
    def visit_list(cls, argument, interface, state):
        new_argument = deepcopy(argument)
        new_argument['type'] = argument['type']['ofType']

        option, _ = cls.generate_node(new_argument, interface, state)
        #option.nargs = '?'
        option.type=None,
        option.action = 'callback'
        option.callback = cls.list_callback
        return option

    @classmethod
    def visit_list(cls, argument, interface, state):
        new_argument = deepcopy(argument)
        new_argument['type'] = argument['type']['ofType']
        cls.generate_node(new_argument, interface, state, depart=False)
        kwargs = state['kwargs']
        kwargs['action'] = 'callback'
        kwargs['callback'] = cls.list_callback
        # NOTE: the type must be None else the first argument will be swallowed
        kwargs['callback_kwargs'] = {'type_': kwargs['type']}
        kwargs['type'] = None
        # NOTE: the dest must be manually set for a list
        kwargs['dest'] = argument['name']

    depart_list = make_option

    @staticmethod
    def list_callback(option, _, value, parser, type_):
        ret = []
        for arg in parser.rargs:
            if arg[0] == '-':
                break
            try:
                ret.append(type_(arg))
            except (ValueError, TypeError): 
                raise  # TODO: better error capture
            print('#', arg)
        del parser.rargs[:len(ret)]
        setattr(parser.values, option.dest, ret)


mutation = {
    'name': 'mymutation',
    'description': 'Mutate this that and everything.',
    'args': [
        {
            'name': 'foo',
            'description': 'Foo',
            'type': {
                'name': None,
                'kind': 'NON_NULL',
                'ofType':  {
                    'name': 'String',
                    'kind': 'SCALAR'
                }
            }
        },
        {
            'name': 'bar',
            'description': 'Bar',
            'type': {
                'name': 'String',
                'kind': 'SCALAR'
            }
        },
        {
            'name': 'baz',
            'description': 'Baz',
            'type': {
                'name': 'Int',
                'kind': 'SCALAR'
            }
        },
        {
            'name': 'pub',
            'description': 'Pub',
            'type': {
                'name': None,
                'kind': 'NON_NULL',
                'ofType': {
                    'name': None,
                    'kind': 'LIST',
                    'ofType': {
                        'name': 'Int',
                        'kind': 'SCALAR'
                    }
                }
            }
        }
    ]
}

parser = OptParseInterfaceGenerator.generate(mutation)
parser.print_help()
args, opts = parser.parse_args([
    'foo',
    '2', '3', '4',
    '--bar', 'bar',
    '--baz', '1',

])
print(args)
print(opts)
