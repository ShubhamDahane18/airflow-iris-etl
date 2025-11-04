"""
Test suite for Airflow DAGs
"""
import sys
import os
from pathlib import Path

# Add dags directory to path
dags_dir = Path(__file__).parent.parent / 'dags'
sys.path.insert(0, str(dags_dir))


def test_dag_import():
    """Test that DAG can be imported without errors"""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'iris_ml_pipeline',
            str(dags_dir / 'iris_ml_pipeline.py')
        )
        dag_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dag_module)
        dag = dag_module.dag
        assert dag is not None
        print("✓ DAG imported successfully")
    except Exception as e:
        raise AssertionError(f"Failed to import DAG: {e}")


def test_dag_id():
    """Test DAG has correct ID"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'iris_ml_pipeline',
        str(dags_dir / 'iris_ml_pipeline.py')
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    dag = dag_module.dag
    
    assert dag.dag_id == 'iris_ml_pipeline', f"Expected 'iris_ml_pipeline', got '{dag.dag_id}'"
    print(f"✓ DAG ID correct: {dag.dag_id}")


def test_dag_has_tasks():
    """Test DAG has tasks"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'iris_ml_pipeline',
        str(dags_dir / 'iris_ml_pipeline.py')
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    dag = dag_module.dag
    
    assert len(dag.tasks) > 0, "DAG has no tasks"
    print(f"✓ DAG has {len(dag.tasks)} tasks")


def test_required_tasks_exist():
    """Test that all required tasks exist"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'iris_ml_pipeline',
        str(dags_dir / 'iris_ml_pipeline.py')
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    dag = dag_module.dag
    
    required_tasks = [
        'extract_iris_dataset',
        'feature_engineering',
        'train_test_split',
        'train_models',
        'evaluate_models',
        'check_model_performance',
        'deploy_model',
        'retrain_notification',
        'load_to_database',
        'generate_ml_report'
    ]
    
    task_ids = [task.task_id for task in dag.tasks]
    
    for required_task in required_tasks:
        assert required_task in task_ids, f"Missing required task: {required_task}"
    
    print(f"✓ All {len(required_tasks)} required tasks exist")


def test_dag_has_no_cycles():
    """Test that DAG has no cycles"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'iris_ml_pipeline',
        str(dags_dir / 'iris_ml_pipeline.py')
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    dag = dag_module.dag
    
    try:
        dag.test_cycle()
        has_cycle = False
    except Exception:
        has_cycle = True
    
    assert not has_cycle, "DAG has cycles"
    print("✓ DAG has no cycles")


def test_dag_tags():
    """Test DAG has appropriate tags"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'iris_ml_pipeline',
        str(dags_dir / 'iris_ml_pipeline.py')
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    dag = dag_module.dag
    
    assert hasattr(dag, 'tags'), "DAG has no tags"
    assert 'ml' in dag.tags, "DAG missing 'ml' tag"
    print(f"✓ DAG tags: {dag.tags}")


if __name__ == '__main__':
    print("Running DAG tests...\n")
    
    tests = [
        test_dag_import,
        test_dag_id,
        test_dag_has_tasks,
        test_required_tasks_exist,
        test_dag_has_no_cycles,
        test_dag_tags
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            print(f"\nRunning: {test.__name__}")
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ ERROR: {e}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"Test Results: {passed} passed, {failed} failed")
    print(f"{'='*60}\n")
    
    if failed > 0:
        sys.exit(1)