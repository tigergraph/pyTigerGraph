from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema


class pyTigerGraphUDT(pyTigerGraphSchema):
    """User Defined Typle (UDT) Functions.

    Get information about User Defined Function for the graph.
    """

    def getUDTs(self) -> list:
        """Returns the list of User-Defined Tuples (names only).

        For information on UDTs see xref:gsql-ref:ddl-and-loading:system-and-language-basics.adoc#typedef-tuple[User-Defined Tuple]

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).
        """
        ret = []
        for udt in self._getUDTs():
            ret.append(udt["name"])
        return ret

    def getUDT(self, udtName: str) -> list:
        """Returns the details of a specific User-Defined Tuple (defined in the global scope).

        For information on UDTs see xref:gsql-ref:ddl-and-loading:system-and-language-basics.adoc#typedef-tuple[User-Defined Tuple]

        Args:
            udtName:
                The name of the User-Defined Tuple.

        Returns:
            The metadata (the details of the fields) of the UDT.

        """
        for udt in self._getUDTs():
            if udt["name"] == udtName:
                return udt["fields"]
        return []  # UDT was not found
        # TODO Should raise exception instead?
