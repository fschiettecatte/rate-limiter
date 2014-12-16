#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#--------------------------------------------------------------------------
#
# Author: Francois Schiettecatte
# Creation Date: December 2014 (originally created December 2010)
#


#--------------------------------------------------------------------------
#
# Description:
#
# Limiter class
#


#--------------------------------------------------------------------------
#
# Imported modules
#

import time


#--------------------------------------------------------------------------
#
# Constants
#


#
# Limiter status
#

# Not blocked
STATUS_NO_BLOCK = 0

# Short block - blocked until the rate drops back below the maximum rate
STATUS_SHORT_BLOCK = 1

# Extended block - blocked for an extended period of time if we rate was exceeded 
# too many times
STATUS_EXTENDED_BLOCK = 2



#
# Algorithm 1
#
# Rate increases with every request and decreases over time, more requests just increases
# the rate meaning the client will have to wait longer to be allowed back in.
#
# Also has the option of blocking a client if they exceed the maximum rate too often.
#

# Algorithm ID
_A1_ALGORITHM_ID = 1

# Maximum rate (requests per second)
_A1_MAXIMUM_RATE = 4.0

# Maximum excesses before we trigger an extended block (-1 for no maximum)
_A1_MAXIMUM_EXCESSES = 30

# Extended block expiration in seconds (6 hours)
_A1_EXTENDED_BLOCK_EXPIRATION = 21600

# Expiration in seconds (1 hour)
_A1_EXPIRATION = 3600



#
# Algorithm 2
# 
# Based on:
#
#   http://stackoverflow.com/questions/667508/whats-a-good-rate-limiting-algorithm
#
# This caps the number of requests over a time period
#
# Also has the option of blocking a client if they exceed the maximum rate too often.
#

# Algorithm ID
_ALGORITHM_ID_A2 = 2

# Number of requests
_A2_REQUESTS = 10.0

# Period in seconds
_A2_PERIOD = 5.0

# Allowance, number of requests within a period
_A2_ALLOWANCE = (_A2_REQUESTS / _A2_PERIOD)

# Maximum excesses before we trigger an extended block (-1 for no maximum)
_A2_MAXIMUM_EXCESSES = 30

# Extended block expiration in seconds (6 hours)
_A2_EXTENDED_BLOCK_EXPIRATION = (60 * 60 * 6)

# Expiration in seconds (1 hour)
_A2_EXPIRATION = 3600



#
# Algorithm 3
#
# No rate limit
#

# Algorithm ID
_A3_ALGORITHM_ID = 3



# Select which algorithm we want to use
_ALGORITHM_ID = _A3_ALGORITHM_ID



#--------------------------------------------------------------------------
#
# Setup
#

# Set the locale
locale.setlocale(locale.LC_ALL, 'en_US')

# Logging format - '2014-02-21 09:37:25,480 CRITICAL - ...'
LOGGING_FORMAT = '%(asctime)s %(levelname)s - %(message)s'

# Logging basic config
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)


#--------------------------------------------------------------------------
#
# Globals
#

# Logger
logger = logging.getLogger()


#--------------------------------------------------------------------------
#
#   Class:      Limiter
#
#   Purpose:    Limiter class
#
class Limiter (object):


    #--------------------------------------------------------------------------
    #
    #   Method:         __init__
    #
    #   Purpose:        Constructor
    #
    #   Parameters:    
    #
    #   Exceptions:    
    #
    def __init__(self):

        # Initialize the database we use to store rate data, 
        # replace this with something worthy
        self._database = None

        

    #--------------------------------------------------------------------------
    #
    #   Method:         incrementRate
    #
    #   Purpose:        Increment the rate
    #
    #   Parameters:     clientIdentifier    client identifier
    #
    #   Exceptions:     ValueError          Missing or invalid client identifier
    #                   ValueError          No algorithm was selected
    #
    #   Returns:        Limiter status
    #
    def incrementRate (self, clientIdentifier):

        # Check the parameters
        if not clientIdentifier:
            raise ValueError('Missing or invalid client identifier')

    
        # Route to the appropriate algorithm, otherwise raise an error
        if _ALGORITHM_ID == _A1_ALGORITHM_ID:
            return self._A1_incrementRate(clientIdentifier)

        elif _ALGORITHM_ID == _ALGORITHM_ID_A2:
            return self._A2_incrementRate(clientIdentifier)

        elif _ALGORITHM_ID == _A3_ALGORITHM_ID:
            return STATUS_NO_BLOCK

        else:
            raise ValueError('No algorithm was selected')



    #--------------------------------------------------------------------------
    #
    #   Method:         _A1_incrementRate
    #
    #   Purpose:        Increment the rate - algoritm 1
    #
    #   Parameters:     clientIdentifier    client identifier
    #
    #   Exceptions:     ValueError          Missing or invalid client identifier
    #
    #   Returns:        Limiter status
    #
    def _A1_incrementRate (self, clientIdentifier):

        #
        # Value fields:
        #
        #   rate        -   the current rate
        #   last        -   the last time.time() at which the rate was incremented
        #   excesses    -   the number of times the maximum rate was exceeded
        #   status      -   the status
        #

        # Rate, last, excesses and status, initial values
        rate, last, excesses, status = (0, 0, 0, STATUS_NO_BLOCK)

                
        # Get the data from the database
        data = self._get(clientIdentifier)

        # Unpack the data, checking the status for a permanent block, return here if that is the case
        if data:
            rate, last, excesses, status = data[0], data[1], data[2], data[3]
            if status == STATUS_EXTENDED_BLOCK:
                return status


        # Get the time now
        now = time.time()

        # Calculate the new rate
        rate = (1 + rate) / ((now - last) + 1)

        # Maximum rate was exceeded
        if rate >= _A1_MAXIMUM_RATE:
            
            # We newly exceeded the rate so we increment the excesses
            if status == STATUS_NO_BLOCK:
                excesses += 1
            
            # Set the status depending on whether the excesses exceeded the maximum excesses or not
            if _A1_MAXIMUM_EXCESSES != -1 and excesses > _A1_MAXIMUM_EXCESSES:
                status = STATUS_EXTENDED_BLOCK
            else:
                status = STATUS_SHORT_BLOCK
            

        # Set the expiration, override with extended block expiration if needed
        expiration = _A1_EXPIRATION
        if status == STATUS_EXTENDED_BLOCK:
            expiration = _A1_EXTENDED_BLOCK_EXPIRATION

        # Store the data in the database
        self._set(self.clientIdentifier, (rate, now, excesses, status), expiration)
        logger.debug('Rate.increment - rate: [%s], last: [%s], excesses: [%s], status: [%s].', (rate, last, excesses, status))


        # Return the status
        return status



    #--------------------------------------------------------------------------
    #
    #   Method:         _A2_incrementRate
    #
    #   Purpose:        Increment the rate - algoritm 2
    #
    #   Parameters:     clientIdentifier    client identifier
    #
    #   Exceptions:     ValueError          Missing or invalid client identifier
    #
    #   Returns:        Limiter status
    #
    def _A2_incrementRate (self, clientIdentifier):

        #
        # Fields:
        #
        #   allowance   -   the allowance
        #   last        -   the last time.time() at which the rate was incremented
        #   excesses    -   the number of times the maximum rate was exceeded
        #   status      -   the status
        #

        # Allowance, last, excesses and status, initial values
        allowance, last, excesses, status = (_A2_ALLOWANCE, 0, 0, STATUS_NO_BLOCK)

    
        # Get the data from the database
        data = self._get(clientIdentifier)

        # Unpack the data, checking the status for a permanent block, return here if that is the case
        if data:
            allowance, last, excesses, status = data[0], data[1], data[2], data[3]
            if status == STATUS_EXTENDED_BLOCK:
                return status


        # Get the time now
        now = time.time()

        # Calculate the new allowance
        allowance += (now - last) * _A2_ALLOWANCE

        # Cap the allowance
        if allowance > _A2_REQUESTS:
            allowance = _A2_REQUESTS

        # Check the allowance, less than 1 means we have none
        if allowance < 1.0:
            
            # We newly exceeded the rate so we increment the excesses
            if status == STATUS_NO_BLOCK:
                excesses += 1
            
            # Set the status depending on whether the excesses exceeded the maximum excesses or not
            if _A2_MAXIMUM_EXCESSES != -1 and excesses > _A2_MAXIMUM_EXCESSES:
                status = STATUS_EXTENDED_BLOCK
            else:
                status = STATUS_SHORT_BLOCK

        # We still have some allowance so set the status and decrement the allowance
        else:
            status = STATUS_NO_BLOCK
            allowance -= 1.0


        # Set the expiration, override with extended block expiration if needed
        expiration = _A2_EXPIRATION
        if status == STATUS_EXTENDED_BLOCK:
            expiration = _A2_EXTENDED_BLOCK_EXPIRATION

        # Store the data in the database
        self._set(clientIdentifier, (rate, now, excesses, status), expiration)
        logger.debug('Rate.increment - rate: [%s], last: [%s], excesses: [%s], status: [%s].', (rate, last, excesses, status))


        # Return the status
        return status
    
    
    
    #--------------------------------------------------------------------------
    #
    #   Method:         _get
    #
    #   Purpose:        Get the value for this key
    #
    #   Parameters:     key         the key  
    #
    #   Exceptions:     ValueError  Missing or invalid key
    #
    def _get(self, key):

        # Check the parameters
        if not key:
            raise ValueError('Missing or invalid key')


        # Get the value for this key
        value = self._database.get(key)

        # Return the value
        return value



    #--------------------------------------------------------------------------
    #
    #   Method:         _set
    #
    #   Purpose:        Set the value for this key
    #
    #   Parameters:     key         the key  
    #                   value       the value  
    #                   expiration  the expiration (in seconds) 
    #
    #   Exceptions:     ValueError  Missing or invalid key
    #
    def _set(self, key, value, expiration=0):

        # Check the parameters
        if not key:
            raise ValueError('Missing or invalid key')


        # Store the value for this key, setting expiration
        value = self._database.set(key, value, expiration=expiration)
        
        

#--------------------------------------------------------------------------


