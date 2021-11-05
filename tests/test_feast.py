import time

from feaflow import feast


def test_init(project_feast):
    with feast.init(project_feast) as feast_project:
        feast_project.apply()

    # declar = feast._generate_project_declarations()
    # print(declar)

    # feast -> feature_view
    #          require: table name

    # feast materialize

    # feast apply

    # feast generate project.yml

    # feast serve?
