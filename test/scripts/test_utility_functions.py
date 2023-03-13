from FluxPythonUtils.scripts.utility_functions import compare_n_patch_dict


def test_compare_n_patch_dict(stored_dict, update_dict, update_obj):
    stored_dict_ = stored_dict
    stored_dict_["eligible_brokers"] = [update_obj]
    update_dict_ = update_dict

    update_obj["sec_positions"][0]["sec_id"] = "Sec_id_2"
    update_obj["sec_positions"][0]["positions"][0]["available_size"] = 20
    update_obj["sec_positions"][0]["positions"][0]["allocated_size"] = 20
    update_obj["sec_positions"][0]["positions"][0]["consumed_size"] = 20
    update_obj["sec_positions"][0]["positions"][0]["acquire_cost"] = 20
    update_obj["sec_positions"][0]["positions"][0]["carry_cost"] = 20

    update_dict_["eligible_brokers"] = [update_obj]

    updated_dict = compare_n_patch_dict(stored_dict_, update_dict_)

    stored_dict_["eligible_brokers"] = [update_obj]
    expected_updated_dict = stored_dict_

    assert expected_updated_dict == updated_dict
