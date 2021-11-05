from feaflow.feast import Feast


def test_init(project_feast):
    feast = Feast(project_feast)
    declar = feast._generate_project_declarations()
    print(declar)

    # feast -> feature_view
    #          require: table name

    # feast materialize

    # feast apply

    # feast generate project.yml

    # feast serve?
