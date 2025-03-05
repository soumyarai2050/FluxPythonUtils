import os
from pathlib import PurePath, Path
import mimetypes
from typing import List, Union, Optional, Final, Dict, Set
from collections import defaultdict


class LLMHelper:
    DEFAULT_PROMPT_FILENAME: Final[str] = "llm_prompt.md"
    DEFAULT_EXCLUDES: Final[set[str]] = {
        '__pycache__', '.git', '.idea', '.vscode',
        'venv', 'env', '.env', 'node_modules'
    }

    @staticmethod
    def is_text_file(file_path: PurePath) -> bool:
        """Check if file is a text file based on mimetype"""
        mime_type, _ = mimetypes.guess_type(str(file_path))
        return mime_type is not None and mime_type.startswith('text')

    @staticmethod
    def read_file_content(file_path: PurePath) -> str:
        """Read and return file content with proper error handling"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"Error reading file: {str(e)}"

    @staticmethod
    def should_include(path: Union[Path, PurePath], excluded_names: Set[str]) -> bool:
        """
        Check if a path should be included based on exclusion rules.

        Args:
            path: Path to check
            excluded_names: Set of names to exclude (files or directories)

        Returns:
            bool: True if path should be included, False if it should be excluded
        """
        # Check each part of the path against excluded names
        return not any(part in excluded_names for part in path.parts)

    @classmethod
    def build_file_structure(
            cls,
            paths: List[Union[str, PurePath]],
            excluded_names: Optional[Set[str]] = None
    ) -> Dict[PurePath, Set[PurePath]]:
        """
        Build a dictionary representing the file structure of all supplied paths.

        Args:
            paths: List of paths to process
            excluded_names: Optional set of names to exclude (files or directories)

        Returns:
            Dict mapping directories to their contained files
        """
        structure = defaultdict(set)
        # Combine default excludes with user-provided excludes
        all_excludes = cls.DEFAULT_EXCLUDES | (excluded_names or set())

        def is_excluded(path: Union[Path, PurePath]) -> bool:
            """Check if any part of the path matches excluded names"""
            return any(part in all_excludes for part in path.parts)

        for path in paths:
            base_path = Path(path)
            if is_excluded(base_path):
                continue

            if base_path.is_file():
                if not is_excluded(base_path):
                    structure[base_path.parent].add(base_path)
            elif base_path.is_dir():
                for item in base_path.rglob("*"):
                    # Skip if the item or any of its parents are in excluded names
                    if is_excluded(item):
                        continue
                    if item.is_file():
                        try:
                            relative_item = item.relative_to(base_path)
                            if not is_excluded(relative_item):
                                # Only add the parent directory if it's not excluded
                                parent = base_path / relative_item.parent
                                if not is_excluded(parent):
                                    structure[parent].add(item)
                        except ValueError:
                            continue

        # Filter out any excluded directories that might have slipped through
        return {
            k: v for k, v in structure.items()
            if not is_excluded(k) and v  # Only keep non-empty, non-excluded directories
        }

    @staticmethod
    def format_file_structure(structure: Dict[PurePath, Set[PurePath]]) -> List[str]:
        """
        Convert the file structure dictionary into a formatted string representation.
        """
        output = ["## File Structure\n", "```", "📁 Root"]

        # Find the common root path
        all_paths = [str(k) for k in structure.keys()] + [str(f) for files in structure.values() for f in files]
        if not all_paths:
            return output + ["```\n"]

        # Find the common prefix to all paths
        common_prefix = os.path.commonpath(all_paths)

        # Convert structure to sorted list of tuples and normalize paths
        sorted_structure = []
        for dir_path, files in structure.items():
            try:
                rel_dir = os.path.relpath(str(dir_path), common_prefix)
                rel_files = [os.path.relpath(str(f), common_prefix) for f in files]
                if rel_dir != '.':
                    sorted_structure.append((rel_dir, sorted(rel_files)))
            except ValueError:
                continue

        # Sort by path for consistent output
        sorted_structure.sort(key=lambda x: x[0])

        # Track processed directories to avoid duplication
        processed_dirs = set()

        # Process each directory and its files
        for dir_path, files in sorted_structure:
            # Split the directory path into components
            dir_parts = PurePath(dir_path).parts

            # Add any missing parent directories
            current_path = []
            for part in dir_parts:
                current_path.append(part)
                dir_str = str(PurePath(*current_path))
                if dir_str not in processed_dirs:
                    indent = "|   " * (len(current_path) - 1)
                    output.append(f"{indent}📁 {part}")
                    processed_dirs.add(dir_str)

            # Add files
            base_indent = "|   " * len(dir_parts)
            for file in sorted(files):
                file_name = PurePath(file).name
                if file_name not in processed_dirs:  # Avoid duplicate entries
                    output.append(f"{base_indent}📄 {file_name}")
                    processed_dirs.add(file_name)

        output.append("```\n")
        return output

    @classmethod
    def process_path(
            cls,
            base_path: PurePath,
            excluded_names: Optional[Set[str]] = None
    ) -> List[str]:
        """
        Process a single path (file or directory) and return formatted content.

        Args:
            base_path: Path to process
            excluded_names: Optional set of names to exclude
        """
        content_parts = []
        path = Path(base_path)
        all_excludes = cls.DEFAULT_EXCLUDES | (excluded_names or set())

        def is_excluded(path: Union[Path, PurePath]) -> bool:
            """Check if any part of the path matches excluded names"""
            return any(part in all_excludes for part in path.parts)

        if is_excluded(path):
            return content_parts

        if path.is_file():
            if cls.is_text_file(base_path) and not is_excluded(base_path):
                content = cls.read_file_content(path)
                content_parts.extend([
                    f"### File: {base_path.name}\n",
                    "```\n",
                    f"{content}\n",
                    "```\n"
                ])
        elif path.is_dir():
            for item in path.rglob("*"):
                if is_excluded(item):
                    continue
                if item.is_file() and cls.is_text_file(item):
                    content = cls.read_file_content(item)
                    try:
                        relative_path = PurePath(item).relative_to(base_path)
                        if not is_excluded(relative_path):
                            content_parts.extend([
                                f"### File: {relative_path}\n",
                                "```\n",
                                f"{content}\n",
                                "```\n"
                            ])
                    except ValueError:
                        if not is_excluded(PurePath(item.name)):
                            content_parts.extend([
                                f"### File: {item.name}\n",
                                "```\n",
                                f"{content}\n",
                                "```\n"
                            ])

        return content_parts

    @classmethod
    def resolve_output_path(cls, output_path: Optional[Union[str, PurePath]]) -> PurePath:
        """Resolve the final output path"""
        if output_path is None:
            return PurePath(cls.DEFAULT_PROMPT_FILENAME)

        pure_path = PurePath(output_path)
        path_obj = Path(pure_path)

        if path_obj.exists() and path_obj.is_dir():
            return pure_path / cls.DEFAULT_PROMPT_FILENAME

        return pure_path

    @classmethod
    def generate_llm_prompt(
            cls,
            text_prompt: str,
            file_paths: List[Union[str, PurePath]],
            output_path: Optional[Union[str, PurePath]] = None,
            excluded_names: Optional[Set[str]] = None
    ) -> None:
        """
        Generates an organized LLM prompt file combining the text prompt and content from multiple files/directories.

        Args:
            text_prompt: The main instruction/prompt text
            file_paths: List of paths to files or directories
            output_path: Optional output path for the generated prompt
            excluded_names: Optional set of names to exclude (files or directories)
        """
        # Initialize the prompt content
        prompt_content: List[str] = [
            "# LLM Prompt Document\n",
            "## Main Instruction\n",
            f"{text_prompt}\n",
        ]

        # Add file structure section
        file_structure = cls.build_file_structure(file_paths, excluded_names)
        prompt_content.extend(cls.format_file_structure(file_structure))

        # Add file contents section
        prompt_content.append("## File Contents\n")

        # Process all provided paths
        for path in file_paths:
            pure_path = PurePath(path)
            prompt_content.extend(cls.process_path(pure_path, excluded_names))

        # Add final instructions
        prompt_content.extend([
            "## Task\n",
            "Please analyze the above files and respond according to the main instruction.\n"
        ])

        # Resolve the output path
        final_output_path = cls.resolve_output_path(output_path)

        # Create parent directories if they don't exist
        parent_dir = Path(final_output_path).parent
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"Error creating directory structure: {str(e)}")
                return

        # Write to output file
        try:
            with open(final_output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(prompt_content))
            print(f"Successfully generated prompt file: {final_output_path}")
        except Exception as e:
            print(f"Error writing prompt file: {str(e)}")

    @classmethod
    def get_code_review_prompt(cls, focus_dir_names: List[str] | None = None,
                               focus_file_names: List[str] | None = None,
                               focus_class_names: List[str] | None = None,
                               focus_function_names: List[str] | None = None,
                               bug_count: int | None = None,
                               bug_type: str = "obvious"):  # used if bug_count sent; "impacting", "relevant", "trivial"
        focus_dir_names_str = f"with focus on directories: {focus_dir_names} " if focus_dir_names else ""
        focus_file_names_str = f"with focus on files: {focus_file_names} " if focus_file_names else ""
        focus_class_names_str = f"with focus on classes: {focus_class_names} " if focus_class_names else ""
        focus_function_names_str = f"with focus on functions: {focus_function_names} " if focus_function_names else ""
        bug_count_str = f" {bug_count} most {bug_type}" if bug_count else " any"

        return (f"Review the codebase {focus_dir_names_str}{focus_file_names_str}{focus_class_names_str}"
                f"{focus_function_names_str}to find any bugs and generate bug fixed version of files; also generate "
                f"pytests to validate before and after behaviour of the bug found/fixed in code. Python version used"
                f" for running the codebase is >= 3.10")


if __name__ == "__main__":
    def main():
        UTILS_SCRIPT_DIR = PurePath(__file__).parent
        UTILS_DATA_DIR = PurePath(__file__).parent.parent / "data"

        # generate_llm_prompt: single file/dir prompt example
        LLMHelper.generate_llm_prompt(
            LLMHelper.get_code_review_prompt(),
            [UTILS_SCRIPT_DIR / "ws_reader_lite.py"],
            UTILS_DATA_DIR / "ws_reader_lite_review_prompt.md"
        )

        # generate_llm_prompt: multi file/dir mixed prompt example [+ mixed string and PurePath objects]
        code_review_paths = [
            UTILS_SCRIPT_DIR / "ws_reader_lite.py",
            UTILS_SCRIPT_DIR / "ws_reader.py",
            UTILS_SCRIPT_DIR / "yaml_importer.py",
            UTILS_SCRIPT_DIR / "model_base_utils.py",
            UTILS_SCRIPT_DIR.parent / "log_analyzer"
        ]

        LLMHelper.generate_llm_prompt(
            LLMHelper.get_code_review_prompt(),
            code_review_paths,
            UTILS_DATA_DIR / "ws_and_log_analyzer_review_prompt.md"
        )

        # generate_llm_prompt: email_adapter prompt [+mixed string and PurePath objects]
        code_review_paths = [
            UTILS_SCRIPT_DIR / "general_utility_functions.py",
            UTILS_SCRIPT_DIR.parent / "email_adapter"
        ]

        LLMHelper.generate_llm_prompt(
            LLMHelper.get_code_review_prompt(focus_file_names=["email_client.py", "email_handler.py"]),
            code_review_paths,
            UTILS_DATA_DIR / "email_adapter_review_prompt.md",
            excluded_names={"test.jpg", "test.txt"}
        )
        pass

    main()