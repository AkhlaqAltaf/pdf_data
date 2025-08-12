import os
import shutil
import jazzmin
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Pull Jazzmin admin templates and static files into the local project for customization.'

    def handle(self, *args, **options):
        # Locate Jazzmin's package directory
        jazzmin_path = os.path.dirname(jazzmin.__file__)

        # Source paths from Jazzmin package
        jazzmin_templates_src = os.path.join(jazzmin_path, 'templates', 'admin')
        jazzmin_static_src = os.path.join(jazzmin_path, 'static', 'jazzmin')

        # Project root (edit if your layout is different)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # Local destinations
        templates_dest = os.path.join(project_root, 'templates', '../../../templates/admin')
        static_dest = os.path.join(project_root, 'static', '../../../static/admin')

        # Ensure destination folders exist
        os.makedirs(templates_dest, exist_ok=True)
        os.makedirs(static_dest, exist_ok=True)

        # Copy templates
        if os.path.exists(jazzmin_templates_src):
            shutil.copytree(jazzmin_templates_src, templates_dest, dirs_exist_ok=True)
            self.stdout.write(self.style.SUCCESS('✅ Jazzmin templates pulled to local /templates/admin'))

        # Copy static files
        if os.path.exists(jazzmin_static_src):
            shutil.copytree(jazzmin_static_src, static_dest, dirs_exist_ok=True)
            self.stdout.write(self.style.SUCCESS('✅ Jazzmin static files pulled to local /static/jazzmin'))

        self.stdout.write(self.style.SUCCESS('🎉 Jazzmin assets successfully pulled into your project.'))
