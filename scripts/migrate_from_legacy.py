# scripts/migrate_from_legacy.py
import shutil
import os
from pathlib import Path

def migrate_legacy_code():
    """Migrate from legacy master.py structure to new modular structure"""
    
    # Create new directory structure
    dirs_to_create = [
        'src/downloaders',
        'src/processors', 
        'src/forecasters',
        'src/uploaders',
        'src/monitoring',
        'tests',
        'logs',
        'data'
    ]
    
    for dir_path in dirs_to_create:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        Path(dir_path, '__init__.py').touch(exist_ok=True)
    
    print("✅ Directory structure created")
    
    # Copy static files
    if Path('static').exists():
        print("✅ Static files already exist")
    else:
        print("⚠️  Create static/ directory and add TV2.ttf font")
    
    # Copy environment template
    if not Path('.env').exists() and Path('.env.example').exists():
        shutil.copy('.env.example', '.env')
        print("✅ Environment file template created")
    
    print("\n🚀 Migration completed!")
    print("\nNext steps:")
    print("1. Update .env with your actual configuration")
    print("2. Install requirements: pip install -r requirements.txt") 
    print("3. Run tests: pytest")
    print("4. Start application: python -m src.main")

if __name__ == "__main__":
    migrate_legacy_code()