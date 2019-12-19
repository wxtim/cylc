import argparse
from copy import deepcopy
import optparse

from cylc.flow.task_pool import TaskPool


class DropArg(Exception): pass


class InterfaceGenerator():

    @classmethod
    def generate(cls, mutation, types=None, drop_types=None):
        state = {
            'types': types or {},
            'drop types': drop_types or []
        }  # TODO: types some other way?
        interface = cls.visit(mutation, state)
        for argument in mutation['args']:
            cls.generate_node(argument, interface, state)
        depart = getattr(cls, 'depart', None)
        if depart:
            depart(mutation, interface, state)
        return interface

    @classmethod
    def generate_node(cls, argument, interface, state, depth=0):
        try:
            cls._generate_node(argument, interface, state, depth=depth)
        except DropArg:
            # if a type matches the drop argument criterion we raise DropArg
            # and let it float to the top of the generate_node call stack
            if depth == 0:
                # then return to skip the node
                return
            raise

    @classmethod
    def _generate_node(cls, argument, interface, state, depth=0):
        visit, depart = cls.get_route(argument, state)
        print(('    ' * depth) + visit.__name__)
        visit(argument, interface, state, depth)
        if 'ofType' in argument['type'] and argument['type']['ofType']:
            new_argument = deepcopy(argument)
            new_argument['type'] = argument['type']['ofType']
            cls.generate_node(new_argument, interface, state, depth=depth + 1)
        if depart:
            print(('    ' * depth) + depart.__name__)
            depart(argument, interface, state, depth)

    @classmethod
    def get_route(cls, argument, state):
        if 'type' in argument:
            type_ = argument['type']
        else:
            type_ = argument['ofType']
        if type_['name'] in state['drop types']:
            raise DropArg
        #name = (type_['name'] or type_['kind']).lower()
        for attr in ('name', 'kind'):
            type_id = type_[attr]
            if not type_id:
                continue
            type_id = type_id.lower()
            try:
                return (
                    getattr(cls, f'visit_{type_id}'),
                    getattr(cls, f'depart_{type_id}', getattr(
                        cls, 'default_depart', print))
                )
            except AttributeError:
                continue
        else:
            print(f'Unsupported type "{type_["kind"]}", falling back to SCALAR.')
            new_argument = dict(argument)
            new_argument['type']['name'] = None
            new_argument['type']['kind'] = 'SCALAR'
            return cls.get_route(new_argument, state)

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
        parser = optparse.OptionParser(
            prog=f'cylc flow <suite> {mutation["name"]}'
        )
        state['usage'] = {}
        return parser

    @classmethod
    def depart(cls, mutation, interface, state):
        usage = interface.usage
        if state['usage']:
            usage += ' ' + (' '.join(state['usage']))

        description = mutation['description'] or ''
        max_width = max((len(sig) for sig in state['usage']))
        lead = 2
        pad = 4
        if state['usage']:
            description += '\n\nArguments:\n'
            description += '\n'.join((
                # leading whitespace
                ' ' * lead
                # name of argument
                + sig
                # space between argument and description
                + ' ' * ((max_width - len(sig)) + pad)
                # desciption
                + ('\n' + (' ' * (lead + max_width + pad))).join(usage_lines)
                for sig, usage_lines in state['usage'].items()
            ))

        interface.usage = usage + '\n\n' + description

    @classmethod
    def default_depart(cls, argument, interface, state, depth):
        if depth > 0:
            return
        interface.add_option(
            *state['args'],
            **state['kwargs']
        )

    @classmethod
    def visit_non_null(cls, argument, interface, state, depth):
        pass

    @classmethod
    def depart_non_null(cls, argument, interface, state, depth):
        usage_sig = argument['name'].upper()
        usage_lines = (argument['description'] or '').splitlines()

        kwargs = state['kwargs']
        if kwargs.get('callback') == cls.list_callback:
            usage_sig += '...'
            list_type = kwargs.get('callback_kwargs', {}).get('type_')
            usage_lines.extend([f'type: {list_type.__name__}...'])

        state['usage'][usage_sig] = usage_lines

    @classmethod
    def visit_scalar(cls, argument, interface, state, depth):
        state['args'] = (
            f'--{argument["name"]}',
        )
        state['kwargs'] = {
            'help': argument['description'],
            'type': cls.TYPE_MAP.get(argument['type']['name'], str),
            'metavar': (
                argument['type']['name']
            )
        }

    @classmethod
    def visit_list(cls, argument, interface, state, level):
        pass

    @classmethod
    def depart_list(cls, argument, interface, state, level):
        new_argument = deepcopy(argument)
        new_argument['type'] = argument['type']['ofType']
        kwargs = state['kwargs']
        kwargs['action'] = 'callback'
        kwargs['callback'] = cls.list_callback
        # NOTE: the type must be None else the first argument will be swallowed
        kwargs['callback_kwargs'] = {'type_': kwargs['type']}
        kwargs['type'] = None
        # NOTE: the dest must be manually set for a list
        kwargs['dest'] = argument['name']
        if level == 0:
            cls.default_depart(argument, interface, state, level)

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
        del parser.rargs[:len(ret)]
        setattr(parser.values, option.dest, ret)

    @classmethod
    def visit_enum(cls, argument, interface, state, level):
        cls.visit_scalar(argument, interface, state, level)
        state['kwargs'].update({
            'type': 'choice',
            'choices': [
                option['name'] for option in argument['type']['enumValues']
            ]
        })


class ArgParseInterfaceGenerator(InterfaceGenerator):

    TYPE_MAP = {
        'String': str,
        'Int': int
    }

    @classmethod
    def visit(cls, mutation, state):
        parser = argparse.ArgumentParser(
            prog=f'cylc flow <suite> {mutation["name"]}'
        )
        cls.reset_state(state)
        return parser

    @staticmethod
    def reset_state(state):
        state['args'] = []
        state['kwargs'] = {}

    @classmethod
    def default_depart(cls, argument, interface, state, depth):
        if depth > 0:
            return
        interface.add_argument(
            *state['args'],
            **state['kwargs']
        )
        cls.reset_state(state)

    @classmethod
    def visit_non_null(cls, argument, interface, state, depth):
        pass

    @classmethod
    def depart_non_null(cls, argument, interface, state, depth):
        # strip '--' from arg
        if not state['args']:
            breakpoint()
        name = state['args'][0]
        if name.startswith('--'):
            name = name[2:]
        elif name.startswith('-'):
            name = name[1:]
        state['args'][0] = name

        # metavar gets used as the argument name if set
        state['kwargs']['metavar'] = None

        if depth == 0:
            cls.default_depart(argument, interface, state, depth)

    @classmethod
    def visit_scalar(cls, argument, interface, state, depth):
        state['args'].extend([
            f'--{argument["name"]}'
        ])
        state['kwargs'].update({
            'help': argument['description'],
            'type': cls.TYPE_MAP.get(argument['type']['name'], str),
            'metavar': (
                argument['type']['name']
            )
        })

    @classmethod
    def visit_list(cls, argument, interface, state, level):
        state['kwargs']['nargs'] = '+'

    @classmethod
    def visit_enum(cls, argument, interface, state, level):
        cls.visit_scalar(argument, interface, state, level)
        state['kwargs'].update({
            'choices': [
                option['name'] for option in argument['type']['enumValues']
            ],
            'type': None
        })

    # TODO: don't provide special handling for input_object
    #       but do add some documentation support?
    #@classmethod
    #def visit_input_object(cls, argument, interface, state, level):
    #    name = argument['type']['name']
    #    for typ in state['types']:
    #        if typ['name'] == name:
    #            break
    #    else:
    #        breakpoint()
    #        raise ValueError  # TODO!!!

    #    for input_field in typ['inputFields']:
    #        child_state = deepcopy(state)
    #        cls.reset_state(child_state)
    #        input_argument = deepcopy(argument)
    #        input_argument['type'] = input_field['type']
    #        print(input_argument)
    #        cls.generate_node(input_argument, interface, child_state, 0)

    @classmethod
    def visit_namespaceidglob(cls, argument, interface, state, level):
        # namespace.cycle:status
        cls.visit_scalar(argument, interface, state, level)

    @classmethod
    def depart_namespaceidglob(cls, argument, interface, state, level):
        for typ in state['types']:
            if typ['name'] == 'NamespaceIDGlob':
                break
        else:
            raise ValueError
        state['kwargs'].update({
            'action': cls.NameSpaceIDGlobAction,
            #'const': argument
            'const': typ
        })
        cls.default_depart(argument, interface, state, level)

    class NameSpaceIDGlobAction(argparse.Action):
        def __init__(self, *args, const=None, **kwargs):
            argparse.Action.__init__(self, *args, **kwargs)
            self._type = const

        def __call__(self, parser, namespace, values, _, **kwargs):
            value = dict(zip(
                #[
                #    field['name']
                #    for field in self._type['inputFields']
                #],
                ['cycle', 'namespace', 'status'],
                TaskPool.parse_namespace_glob(values[0])
            ))
            setattr(namespace, self.dest, value)

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
        },
        {
            'name': 'qux',
            'description': 'Qux',
            'type': {
                'name': None,
                'kind': 'LIST',
                'ofType': {
                    'name': 'Int',
                    'kind': 'SCALAR'
                }
            }
        },
        {
            'name': 'oxx',
            'description': 'Oxx',
            'type': {
                'name': 'MyEnum',
                'kind': 'ENUM',
                'enumValues': [
                    {'name': 'a'},
                    {'name': 'b'},
                    {'name': 'c'}
                ]
            }
        }
    ]
}

def test():
    parser = ArgParseInterfaceGenerator.generate(mutation)
    parser.print_help()
    args = parser.parse_args([
        'foo',
        '2', '3', '4',
        '--bar', 'bar',
        '--baz', '1',
        '--qux', '2', '3', '4',
        '--oxx', 'c'
    ])
    print(args)

from cylc.flow.task_pool import TaskPool
#test()
