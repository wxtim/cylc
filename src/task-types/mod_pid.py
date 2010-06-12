#!/usr/bin/env python

#         __________________________
#         |____C_O_P_Y_R_I_G_H_T___|
#         |                        |
#         |  (c) NIWA, 2008-2010   |
#         | Contact: Hilary Oliver |
#         |  h.oliver@niwa.co.nz   |
#         |    +64-4-386 0461      |
#         |________________________|


import re

class pid:
    # PREVIOUS INSTANCE DEPENDENCE FOR FORECAST MODELS

    is_tied = True  # used in manager

    # Forecast models depend on a previous instance via their restart
    # files. This class provides a method to register special restart
    # prerequisites and outputs, and overrides
    # free.ready_to_spawn() appropriately.

    def register_restart_outputs( self, n_restart_outputs ):
        # call after parent init, so that self.c_time is defined!

        msg = self.name + ' restart files ready for '
        self.prerequisites.add(  msg + self.c_time )

        rt = self.c_time
        for n in range( n_restart_outputs ):
            next_rt = self.next_c_time( rt )
            self.outputs.add( msg + next_rt )
            rt = next_rt

    def set_next_restart_completed( self ):
        restart_messages = []
        for message in self.outputs.satisfied.keys():
            if re.search( 'restart files ready for', message ):
                restart_messages.append( message )

        restart_messages.sort()
        for message in restart_messages:
            if not self.outputs.is_satisfied( message ):
                self.incoming( 'NORMAL', message )
                # that's the next one, quit now.
                break


    def set_all_restarts_completed( self ):
        # convenience for external tasks that don't report restart
        # outputs one at a time.
        self.log( 'WARNING', 'setting ALL restart outputs completed' )
        for message in self.outputs.satisfied.keys():
            if re.search( 'restart files ready for', message ):
                if not self.outputs.is_satisfied( message ):
                    self.incoming( 'NORMAL', message )
 

    def ready_to_spawn( self ):
        # Never spawn a waiting task of this type because the
        # successor's restart prerequisites could get satisfied by the
        # later restart outputs of an earlier previous instance, and
        # thereby start too soon (we want this to happen ONLY if the
        # previous task fails and is subsequently made to spawn and 
        # die by the system operator).

        if self.has_spawned():
            # already spawned
            return False

        if self.state.is_waiting() or self.state.is_submitted():
            return False

        if self.state.is_finished():
            # always spawn a finished task
            return True

        ready = False

        if self.state.is_running() or self.state.is_failed(): 
            # failed tasks are running before they fail, so will already
            # have spawned, or not, according to whether they fail
            # before or after completing their restart outputs.

            # ready only if all restart outputs are completed
            # as explained above

            ready = True
            for message in self.outputs.satisfied.keys():
                if re.search( 'restart files ready for', message ) and \
                        not self.outputs.satisfied[ message ]:
                    ready = False
                    break

        return ready

    def my_successor_still_needs_me( self, tasks ):
        my_ct = self.c_time
        nx_ct = self.next_c_time()
        my_name = self.name
        for task in tasks:
            if task.name != my_name:
                continue
            if task.c_time != nx_ct:
                continue
            # found my successor
            if task.state.is_finished():
                return False
            else:
                return True

        # TO DO: consider, and observe, if this can ever happen: 
        print "WARNING: FAILED TO FIND THE SUCCESSOR OF A SPAWNED TASK!"
        return False
