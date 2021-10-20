from feaflow.feast import Feast


def test_init(project_feast):
    feast = Feast(project_feast)
    feast.init()
