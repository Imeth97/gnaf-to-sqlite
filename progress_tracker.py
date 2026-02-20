import os
import time
import sys


def format_time(seconds):
    """Convert seconds to human-readable format (e.g., '2m 15s' or '45s')."""
    if seconds is None:
        return "calculating..."

    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds // 60
    remaining_seconds = seconds % 60

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m"


def format_number(num):
    """Add comma separators to numbers (e.g., 125430 -> '125,430')."""
    return f"{num:,}"


class ProgressTracker:
    """Tracks loading progress with single-line updates and ETA calculation.

    Pre-scans all PSV files to count total rows, then tracks progress based on
    actual row processing speed for accurate ETA estimates.
    """

    def __init__(self, state, table_list, data_dir):
        """Initialize progress tracker and pre-scan files to count total rows.

        Args:
            state: State abbreviation (e.g., 'ACT', 'NSW')
            table_list: List of table names to process
            data_dir: Directory containing the PSV files
        """
        self.state = state
        self.table_list = table_list
        self.total_tables = len(table_list)
        self.data_dir = data_dir

        # Pre-scan files to count total rows
        self.total_rows = self._count_total_rows()

        # Initialize counters
        self.cumulative_rows_processed = 0
        self.start_time = time.time()

        # Current progress state
        self.current_table = None
        self.current_table_index = 0

    def _count_total_rows(self):
        """Pre-scan all PSV files to count total rows across all tables.

        Returns:
            Total number of data rows (excluding headers) across all tables
        """
        total = 0
        for table in self.table_list:
            file_path = os.path.join(self.data_dir, f'{self.state}_{table}_psv.psv')
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Count lines minus header
                    row_count = sum(1 for _ in f) - 1
                    total += row_count
            except FileNotFoundError:
                # File doesn't exist, skip it
                continue

        return total

    def update(self, table_name, table_index, rows_in_batch):
        """Update progress and display single-line progress indicator.

        Args:
            table_name: Name of the table currently being processed
            table_index: Index of current table (0-based)
            rows_in_batch: Number of rows processed in this batch
        """
        self.current_table = table_name
        self.current_table_index = table_index
        self.cumulative_rows_processed += rows_in_batch

        # Calculate elapsed time
        elapsed = time.time() - self.start_time

        # Calculate ETA based on processing rate
        if self.cumulative_rows_processed > 0 and elapsed > 0:
            rows_per_second = self.cumulative_rows_processed / elapsed
            remaining_rows = self.total_rows - self.cumulative_rows_processed
            eta_seconds = remaining_rows / rows_per_second if rows_per_second > 0 else None
        else:
            eta_seconds = None

        # Calculate percentage
        if self.total_rows > 0:
            percentage = (self.cumulative_rows_processed / self.total_rows) * 100
        else:
            percentage = 0

        # Format progress line
        progress_line = (
            f"{self.state} | {table_name} ({table_index + 1}/{self.total_tables}) | "
            f"{format_number(self.cumulative_rows_processed)} / {format_number(self.total_rows)} rows | "
            f"{percentage:.1f}% | "
            f"Elapsed: {format_time(elapsed)} | "
            f"ETA: {format_time(eta_seconds)}"
        )

        # Pad to 120 characters to clear previous line artifacts
        progress_line = progress_line.ljust(120)

        # Print with carriage return to overwrite previous line
        sys.stdout.write(f'\r{progress_line}')
        sys.stdout.flush()

    def finish(self):
        """Print final newline to preserve the last progress line."""
        print()  # Move to next line


class AuthorityProgressTracker:
    """Simplified progress tracker for authority code tables (no state, different file naming)."""

    def __init__(self, table_list, data_dir):
        """Initialize progress tracker for authority code tables.

        Args:
            table_list: List of authority table names to process
            data_dir: Directory containing the authority code PSV files
        """
        self.table_list = table_list
        self.total_tables = len(table_list)
        self.data_dir = data_dir

        # Pre-scan files to count total rows
        self.total_rows = self._count_total_rows()

        # Initialize counters
        self.cumulative_rows_processed = 0
        self.start_time = time.time()

        # Current progress state
        self.current_table = None
        self.current_table_index = 0

    def _count_total_rows(self):
        """Pre-scan all authority code PSV files to count total rows.

        Returns:
            Total number of data rows (excluding headers) across all tables
        """
        total = 0
        for table in self.table_list:
            file_path = os.path.join(self.data_dir, f'Authority_Code_{table}_psv.psv')
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Count lines minus header
                    row_count = sum(1 for _ in f) - 1
                    total += row_count
            except FileNotFoundError:
                # File doesn't exist, skip it
                continue

        return total

    def update(self, table_name, table_index, rows_in_batch):
        """Update progress and display single-line progress indicator.

        Args:
            table_name: Name of the authority table currently being processed
            table_index: Index of current table (0-based)
            rows_in_batch: Number of rows processed in this batch
        """
        self.current_table = table_name
        self.current_table_index = table_index
        self.cumulative_rows_processed += rows_in_batch

        # Calculate elapsed time
        elapsed = time.time() - self.start_time

        # Calculate ETA based on processing rate
        if self.cumulative_rows_processed > 0 and elapsed > 0:
            rows_per_second = self.cumulative_rows_processed / elapsed
            remaining_rows = self.total_rows - self.cumulative_rows_processed
            eta_seconds = remaining_rows / rows_per_second if rows_per_second > 0 else None
        else:
            eta_seconds = None

        # Calculate percentage
        if self.total_rows > 0:
            percentage = (self.cumulative_rows_processed / self.total_rows) * 100
        else:
            percentage = 0

        # Format progress line
        progress_line = (
            f"Authority Codes | {table_name} ({table_index + 1}/{self.total_tables}) | "
            f"{format_number(self.cumulative_rows_processed)} / {format_number(self.total_rows)} rows | "
            f"{percentage:.1f}% | "
            f"Elapsed: {format_time(elapsed)} | "
            f"ETA: {format_time(eta_seconds)}"
        )

        # Pad to 120 characters to clear previous line artifacts
        progress_line = progress_line.ljust(120)

        # Print with carriage return to overwrite previous line
        sys.stdout.write(f'\r{progress_line}')
        sys.stdout.flush()

    def finish(self):
        """Print final newline to preserve the last progress line."""
        print()  # Move to next line
