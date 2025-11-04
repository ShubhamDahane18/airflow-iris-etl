# """
# Test suite for Airflow DAGs
# """
# import sys
# import os
# from pathlib import Path

# # Add dags directory to path
# dags_dir = Path(__file__).parent.parent / 'dags'
# sys.path.insert(0, str(dags_dir))


# def test_dag_import():
#     """Test that DAG can be imported without errors"""
#     try:
#         import importlib.util
#         spec = importlib.util.spec_from_file_location(
#             'iris_ml_pipeline',
#             str(dags_dir / 'iris_ml_pipeline.py')
#         )
#         dag_module = importlib.util.module_from_spec(spec)
#         spec.loader.exec_module(dag_module)
#         dag = dag_module.dag
#         assert dag is not None
#         print("✓ DAG imported successfully")
#     except Exception as e:
#         raise AssertionError(f"Failed to import DAG: {e}")


# def test_dag_id():
#     """Test DAG has correct ID"""
#     import importlib.util
#     spec = importlib.util.spec_from_file_location(
#         'iris_ml_pipeline',
#         str(dags_dir / 'iris_ml_pipeline.py')
#     )
#     dag_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(dag_module)
#     dag = dag_module.dag
    
#     assert dag.dag_id == 'iris_ml_pipeline', f"Expected 'iris_ml_pipeline', got '{dag.dag_id}'"
#     print(f"✓ DAG ID correct: {dag.dag_id}")


# def test_dag_has_tasks():
#     """Test DAG has tasks"""
#     import importlib.util
#     spec = importlib.util.spec_from_file_location(
#         'iris_ml_pipeline',
#         str(dags_dir / 'iris_ml_pipeline.py')
#     )
#     dag_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(dag_module)
#     dag = dag_module.dag
    
#     assert len(dag.tasks) > 0, "DAG has no tasks"
#     print(f"✓ DAG has {len(dag.tasks)} tasks")


# def test_required_tasks_exist():
#     """Test that all required tasks exist"""
#     import importlib.util
#     spec = importlib.util.spec_from_file_location(
#         'iris_ml_pipeline',
#         str(dags_dir / 'iris_ml_pipeline.py')
#     )
#     dag_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(dag_module)
#     dag = dag_module.dag
    
#     required_tasks = [
#         'extract_iris_dataset',
#         'feature_engineering',
#         'train_test_split',
#         'train_models',
#         'evaluate_models',
#         'check_model_performance',
#         'deploy_model',
#         'retrain_notification',
#         'load_to_database',
#         'generate_ml_report'
#     ]
    
#     task_ids = [task.task_id for task in dag.tasks]
    
#     for required_task in required_tasks:
#         assert required_task in task_ids, f"Missing required task: {required_task}"
    
#     print(f"✓ All {len(required_tasks)} required tasks exist")


# def test_dag_has_no_cycles():
#     """Test that DAG has no cycles"""
#     import importlib.util
#     spec = importlib.util.spec_from_file_location(
#         'iris_ml_pipeline',
#         str(dags_dir / 'iris_ml_pipeline.py')
#     )
#     dag_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(dag_module)
#     dag = dag_module.dag
    
#     try:
#         dag.test_cycle()
#         has_cycle = False
#     except Exception:
#         has_cycle = True
    
#     assert not has_cycle, "DAG has cycles"
#     print("✓ DAG has no cycles")


# def test_dag_tags():
#     """Test DAG has appropriate tags"""
#     import importlib.util
#     spec = importlib.util.spec_from_file_location(
#         'iris_ml_pipeline',
#         str(dags_dir / 'iris_ml_pipeline.py')
#     )
#     dag_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(dag_module)
#     dag = dag_module.dag
    
#     assert hasattr(dag, 'tags'), "DAG has no tags"
#     assert 'ml' in dag.tags, "DAG missing 'ml' tag"
#     print(f"✓ DAG tags: {dag.tags}")


# if __name__ == '__main__':
#     print("Running DAG tests...\n")
    
#     tests = [
#         test_dag_import,
#         test_dag_id,
#         test_dag_has_tasks,
#         test_required_tasks_exist,
#         test_dag_has_no_cycles,
#         test_dag_tags
#     ]
    
#     passed = 0
#     failed = 0
    
#     for test in tests:
#         try:
#             print(f"\nRunning: {test.__name__}")
#             test()
#             passed += 1
#         except AssertionError as e:
#             print(f"❌ FAILED: {e}")
#             failed += 1
#         except Exception as e:
#             print(f"❌ ERROR: {e}")
#             failed += 1
    
#     print(f"\n{'='*60}")
#     print(f"Test Results: {passed} passed, {failed} failed")
#     print(f"{'='*60}\n")
    
#     if failed > 0:
#         sys.exit(1)

import os
import sys
import pytest
from airflow.models import DagBag

# Ensure the DAGs directory is in Python path
DAGS_DIR = os.path.join(os.getcwd(), "airflow-iris-etl", "dags")
sys.path.append(DAGS_DIR)


@pytest.fixture(scope="module")
def dag_bag():
    """Load the DAG bag from the specified directory."""
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dag_bag):
    """Ensure there are no import errors when loading DAGs."""
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_iris_ml_pipeline_loaded(dag_bag):
    """Verify that the iris_ml_pipeline DAG is successfully loaded."""
    dag_id = "iris_ml_pipeline"
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None, f"DAG '{dag_id}' not found!"
    assert dag.default_args["owner"] == "ml_engineer"


def test_task_count(dag_bag):
    """Ensure DAG contains the expected number of tasks."""
    dag_id = "iris_ml_pipeline"
    dag = dag_bag.get_dag(dag_id)
    expected_task_count = 10  # total tasks defined in your DAG
    assert len(dag.tasks) == expected_task_count, f"Expected {expected_task_count} tasks, found {len(dag.tasks)}"


def test_task_dependencies(dag_bag):
    """Check the main task dependency structure."""
    dag_id = "iris_ml_pipeline"
    dag = dag_bag.get_dag(dag_id)

    # Expected structure
    expected_edges = [
        ("extract_iris_dataset", "feature_engineering"),
        ("feature_engineering", "train_test_split"),
        ("train_test_split", "train_models"),
        ("train_models", "evaluate_models"),
        ("evaluate_models", "check_model_performance"),
        ("check_model_performance", "deploy_model"),
        ("check_model_performance", "retrain_notification"),
        ("deploy_model", "load_to_database"),
        ("retrain_notification", "load_to_database"),
        ("load_to_database", "generate_ml_report"),
    ]

    # Extract actual DAG dependencies
    actual_edges = [(u.task_id, v.task_id) for u, v in dag.edge_list]
    for edge in expected_edges:
        assert edge in actual_edges, f"Missing dependency: {edge}"


def test_schedule_interval(dag_bag):
    """Ensure DAG runs daily."""
    dag = dag_bag.get_dag("iris_ml_pipeline")
    assert dag.schedule_interval == "@daily", "DAG schedule interval should be '@daily'"
