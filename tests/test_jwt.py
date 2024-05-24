import unittest
from unittest.mock import patch

from pyTigerGraph.pyTigerGraph import TigerGraphConnection

class TestTigerGraphConnection(unittest.TestCase):

    def test_jwt_authentication_real(self):
        # conn = TigerGraphConnection(host="http://35.184.199.42", username="tigergraph", password="graphtiger")
        # conn.getToken()
        conn = TigerGraphConnection(
            host="http://35.184.199.42",
            jwtToken="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0aWdlcmdyYXBoIiwiaWF0IjoxNzE2NTgwMTQ0LCJleHAiOjE3MTY1ODE5NDR9.W4GvGoBmHJGgU5GVZI6ZwdIFKhM0BeFhb3yRmtPSSmIJ2AQeFHGjcQXagY98YQwhqC3MRPnsf0-oyORJX3T2w1taS9rrs7saC8H3rehIal0pWJbIM4BsJvwzHxBi-fZMhADjohK6Upv7KezIVLEqs00VHp7bRJ-Oxgf8XESGgizqjvJN5P7b2jaLn4n9MpJdb_g1QXGr1_vBid4ditslX2EkegM3wyXXbvse76qXtuTvkAj_B_fo_9fMZ2j4kqyDdQx4HzyuCEojZ5Ik34YZI1jlW03pDg1ZFux7-ij0vioHq0-spM_HKhCLBHuSl81uiiVcnvmflcQdLppkf4EnLg"
        )
    
        # print (conn.authHeader)
        # print (conn.getEndpoints())
        # print (conn.getVertices("Person"))
        # print (conn.getVertexCount("Person"))
        # print (conn.getVer())
        # print (conn.getSchema())

if __name__ == '__main__':
    unittest.main()