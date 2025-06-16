import os
from pathlib import Path

def write_content_to_file(outfile, file_path, file_title):
    """Write the content of a markdown file to the output file with proper formatting."""
    # Write file header
    outfile.write(f'\n### {file_title}\n\n')
    print(file_path)
    # Read and write file content
    with open(file_path, 'r', encoding='utf-8') as infile:
        content = infile.read()
        # Remove frontmatter if exists
        if content.startswith('---'):
            content = content.split('---', 2)[-1].strip()
        outfile.write(content + '\n\n')

def merge_docs(docs_dir, output_file):
    # Define the preferred order of sections
    section_order = [
        'getting-started',
        'assets', 
        'commands',
        'platforms',
        'ingestion',
        'quality',
        'cloud',
        'vscode-extension'
    ]
    
    # Create output file
    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write header and main index content first
        outfile.write('# Bruin Documentation\n\n')
        
        # First, add the main index.md content
        index_path = os.path.join(docs_dir, 'index.md')
        if os.path.exists(index_path):
            with open(index_path, 'r', encoding='utf-8') as infile:
                content = infile.read()
                # Remove frontmatter if exists
                if content.startswith('---'):
                    content = content.split('---', 2)[-1].strip()
                outfile.write(content + '\n\n')
        
        # Collect all directories and files
        all_dirs = {}
        for root, dirs, files in os.walk(docs_dir):
            rel_path = os.path.relpath(root, docs_dir)
            if rel_path != '.':
                all_dirs[rel_path] = (root, sorted(files))
        
        # Process sections in the specified order
        processed_dirs = set()
        
        for section in section_order:
            for dir_path in sorted(all_dirs.keys()):
                if dir_path.startswith(section):
                    root, files = all_dirs[dir_path]
                    
                    # Write section header
                    section_title = dir_path.replace('/', ' > ').title()
                    outfile.write(f'\n## {section_title}\n\n')
                    
                    # Process markdown files in this directory
                    for file in files:
                        if file.endswith('.md'):
                            file_path = os.path.join(root, file)
                            file_title = os.path.splitext(file)[0].replace('-', ' ').title()
                            write_content_to_file(outfile, file_path, file_title)
                    
                    processed_dirs.add(dir_path)
        
        # Add any remaining directories that weren't in the specified order
        for dir_path in sorted(all_dirs.keys()):
            if dir_path not in processed_dirs:
                root, files = all_dirs[dir_path]
                
                # Write section header
                section_title = dir_path.replace('/', ' > ').title()
                outfile.write(f'\n## {section_title}\n\n')
                
                # Process markdown files in this directory
                for file in files:
                    if file.endswith('.md'):
                        file_path = os.path.join(root, file)
                        file_title = os.path.splitext(file)[0].replace('-', ' ').title()
                        write_content_to_file(outfile, file_path, file_title)

if __name__ == '__main__':
    # Merge docs
    docs_dir = '/Users/sabrikaragonen/Desktop/bruin/bruin/docs'
    output_file = 'merged_documentation.md'
    merge_docs(docs_dir, output_file)
    print(f'Documentation merged into {output_file}') 
