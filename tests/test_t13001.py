from mapping.t13001 import T13001


def test_t13001():
    ps1 = T13001()
    ps1.run('冯乃根', '460025198308294818', '12345678910')
    print(ps1.variables)

    ps2 = T13001()
    ps2.run('郑义', '340111197704195032', '13053066666')
    print(ps2.variables)

    ps3 = T13001()
    ps3.run('盛晓刚', '340222197705152311', '13162363237')
    print(ps3.variables)

    ps4 = T13001()
    ps4.run('云高琼', '422826198009300047', '15347020567')
    print(ps4.variables)

    ps5 = T13001()
    ps5.run('刘金双', '420983197106025610', '13166256990')
    print(ps5.variables)

    ps6 = T13001()
    ps6.run('马晓艳', '410305197903240547', '13585748588')
    print(ps6.variables)
