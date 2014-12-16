Rate Limiter:
=============

This implements two rate limiter algorithms:

- One increases with every request and decreases over time, more requests just increases
the rate meaning the client will have to wait longer to be allowed back in.

- The other caps the number of requests over a time period.

