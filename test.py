import os

def list_files(startpath):
    with open("projectTreeStructure.txt", "w", encoding="utf-8") as text_file:
        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            indent = ' ' * 4 * (level)
            text_file.write('{}{}/\n'.format(indent, os.path.basename(root)))
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                text_file.write('{}{}\n'.format(subindent, f))
        text_file.close()


list_files("D:\GithubProjects\End-to-end_Demand_forecasting_inventory_optimization\src")