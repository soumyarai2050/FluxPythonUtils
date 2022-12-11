from typing import List


class MultiKeyDict(object):

    def __init__(self):
        self._keys = {}
        self._data = {}

    def __str__(self):
        keys_str = ""
        temp_var_list = []
        for key in self._keys:
            if self._keys[key] not in temp_var_list:
                keys_str += f"({key}"
                for k, v in self._keys.items():
                    if self._keys[key] == v and k != key:
                        keys_str += f" ,{k}"
                        temp_var_list.append(v)
                    # Else ignoring as this key is not having value same as of current looping key
                keys_str += f" ,{self._keys[key]}"
                keys_str += f"): {self._data[self._keys[key]]}, "
            # Else ignoring this key as this key has been added already

        keys_str = keys_str[:-2]  # Removing comma after last value

        return keys_str

    def __getitem__(self, key):
        try:
            return self._data[key]
        except KeyError:
            return self._data[self._keys[key]]

    def __setitem__(self, key, val):
        try:
            self._data[self._keys[key]] = val
        except KeyError:
            if isinstance(key, tuple):
                if not key:
                    raise ValueError(u'Empty tuple cannot be used as a key')
                key, other_keys = key[0], key[1:]
            else:
                other_keys = []
            self._data[key] = val
            for k in other_keys:
                self._keys[k] = key

    def dict_items(self):
        unique_keys = set(self._keys.values())
        for key in unique_keys:
            key_list = [k for k in self._keys.keys() if self._keys[k] == key]
            key_list.append(key)
            yield key_list, self._data[key]

    # add new key for an existing key stored in MultiKeyDict
    def add_keys(self, to_key, new_keys):
        if to_key not in self._data:
            to_key = self._keys[to_key]
        if isinstance(new_keys, List):
            for key in new_keys:
                self._keys[key] = to_key
        else:
            self._keys[new_keys] = to_key

    # convert existing dict to MultiKeyDict
    @classmethod
    def from_dict(cls, dic):
        result = cls()
        for key, val in dic.items():
            result[key] = val
        return result


if __name__ == "__main__":
    def test():
        multikey = MultiKeyDict()
        multikey[("Your fav lang?", 1234)] = "Python"
        multikey[567] = "Test"
        multikey.add_keys(567, ["Smt", "Amt"])
        multikey.add_keys("Smt", "Tsq")
        multikey[("test", 1)] = 5

        print(multikey._keys)
        print(multikey._data)
        print(multikey)

        for keys, value in multikey.dict_items():
            print(keys, value)

    test()
