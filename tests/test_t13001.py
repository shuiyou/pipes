from mapping.t13001 import T13001


def test_t13001():
    ps1 = T13001('冯乃根', '460025198308294818', '12345678910')
    ps1.transform()
    print(ps1.variables_result())

    ps2 = T13001('郑义', '340111197704195032', '13053066666')
    ps2.transform()
    print(ps2.variables_result())

    ps3 = T13001('盛晓刚', '340222197705152311', '13162363237')
    ps3.transform()
    print(ps3.variables_result())

    ps4 = T13001('云高琼', '422826198009300047', '15347020567')
    ps4.transform()
    print(ps4.variables_result())

    ps5 = T13001('刘金双', '420983197106025610', '13166256990')
    ps5.transform()
    print(ps5.variables_result())
