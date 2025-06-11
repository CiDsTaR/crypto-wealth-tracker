"""Command-line interface for the Crypto Wealth Tracker application."""

import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

from .main import get_app, WalletTrackerApp
from .config import get_config, SettingsError

# Rich console for pretty output
console = Console()


class DecimalJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal objects."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def handle_async(coro):
    """Decorator to handle async functions in Click commands."""

    def wrapper(*args, **kwargs):
        return asyncio.run(coro(*args, **kwargs))

    return wrapper


def print_error(message: str) -> None:
    """Print error message with styling."""
    console.print(f"❌ [bold red]Error:[/bold red] {message}")


def print_success(message: str) -> None:
    """Print success message with styling."""
    console.print(f"✅ [bold green]Success:[/bold green] {message}")


def print_warning(message: str) -> None:
    """Print warning message with styling."""
    console.print(f"⚠️  [bold yellow]Warning:[/bold yellow] {message}")


def print_info(message: str) -> None:
    """Print info message with styling."""
    console.print(f"ℹ️  [bold blue]Info:[/bold blue] {message}")


@click.group()
@click.option('--config-file', type=click.Path(exists=True), help='Path to configuration file')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              default='INFO', help='Logging level')
@click.option('--dry-run', is_flag=True, help='Perform dry run without making changes')
@click.pass_context
def cli(ctx: click.Context, config_file: Optional[str], log_level: str, dry_run: bool):
    """Crypto Wealth Tracker - Calculate on-chain wealth for Ethereum wallets."""
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = config_file
    ctx.obj['log_level'] = log_level
    ctx.obj['dry_run'] = dry_run

    # Show banner
    console.print(Panel.fit(
        "[bold blue]🏦 Crypto Wealth Tracker[/bold blue]\n"
        "[dim]Calculate on-chain wealth for Ethereum wallets[/dim]",
        border_style="blue"
    ))


@cli.command()
@click.option('--spreadsheet-id', required=True, help='Google Sheets spreadsheet ID')
@click.option('--input-range', default='A:B', help='Input range for wallet addresses (default: A:B)')
@click.option('--output-range', default='A1', help='Output range for results (default: A1)')
@click.option('--input-worksheet', help='Input worksheet name (default: first sheet)')
@click.option('--output-worksheet', help='Output worksheet name (default: input sheet)')
@click.option('--skip-inactive', is_flag=True, default=True,
              help='Skip wallets inactive for more than threshold days')
@click.option('--inactive-days', default=365, type=int,
              help='Inactive threshold in days (default: 365)')
@click.option('--batch-size', default=50, type=int,
              help='Processing batch size (default: 50)')
@click.option('--max-concurrent', default=10, type=int,
              help='Max concurrent requests (default: 10)')
@click.option('--progress/--no-progress', default=True,
              help='Show progress bar')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv']),
              default='table', help='Output format for results summary')
@click.option('--save-results', type=click.Path(),
              help='Save detailed results to file')
@click.pass_context
@handle_async
async def analyze(
        ctx: click.Context,
        spreadsheet_id: str,
        input_range: str,
        output_range: str,
        input_worksheet: Optional[str],
        output_worksheet: Optional[str],
        skip_inactive: bool,
        inactive_days: int,
        batch_size: int,
        max_concurrent: int,
        progress: bool,
        output_format: str,
        save_results: Optional[str]
):
    """Analyze wallets from Google Sheets."""

    dry_run = ctx.obj.get('dry_run', False)

    try:
        # Initialize application
        app = get_app()

        with console.status("[bold blue]Initializing application...") as status:
            await app.initialize()
            status.update("[bold green]Application initialized ✅")

        # Show analysis parameters
        params_table = Table(title="Analysis Parameters", box=box.ROUNDED)
        params_table.add_column("Parameter", style="cyan")
        params_table.add_column("Value", style="white")

        params_table.add_row("Spreadsheet ID", spreadsheet_id)
        params_table.add_row("Input Range", input_range)
        params_table.add_row("Output Range", output_range if not dry_run else "[dim]N/A (dry run)[/dim]")
        params_table.add_row("Batch Size", str(batch_size))
        params_table.add_row("Max Concurrent", str(max_concurrent))
        params_table.add_row("Skip Inactive", "✅ Yes" if skip_inactive else "❌ No")
        params_table.add_row("Inactive Threshold", f"{inactive_days} days")
        params_table.add_row("Dry Run", "✅ Yes" if dry_run else "❌ No")

        console.print(params_table)
        console.print()

        # Process wallets with progress tracking
        if progress:
            with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    console=console
            ) as progress_bar:

                task = progress_bar.add_task("Processing wallets...", total=100)

                # Create progress callback
                def update_progress(batch_progress):
                    percentage = batch_progress.get_progress_percentage()
                    progress_bar.update(task, completed=percentage)

                    # Update description with current stats
                    completed = batch_progress.jobs_completed
                    failed = batch_progress.jobs_failed
                    skipped = batch_progress.jobs_skipped
                    total_value = batch_progress.total_value_processed

                    description = (
                        f"Processing wallets... "
                        f"✅{completed} ❌{failed} ⏭️{skipped} "
                        f"💰${total_value:,.0f}"
                    )
                    progress_bar.update(task, description=description)

                # Run analysis
                results = await app.process_wallets_from_sheets(
                    spreadsheet_id=spreadsheet_id,
                    input_range=input_range,
                    output_range=output_range if not dry_run else None,
                    input_worksheet=input_worksheet,
                    output_worksheet=output_worksheet if not dry_run else None,
                    dry_run=dry_run
                )

                progress_bar.update(task, completed=100, description="✅ Analysis completed!")

        else:
            # Run without progress bar
            console.print("🚀 Starting wallet analysis...")
            results = await app.process_wallets_from_sheets(
                spreadsheet_id=spreadsheet_id,
                input_range=input_range,
                output_range=output_range if not dry_run else None,
                input_worksheet=input_worksheet,
                output_worksheet=output_worksheet if not dry_run else None,
                dry_run=dry_run
            )

        # Display results
        await _display_results(results, output_format, save_results)

        if dry_run:
            print_info("This was a dry run - no data was written to Google Sheets")
        else:
            print_success(f"Results written to Google Sheets: {spreadsheet_id}")

    except Exception as e:
        print_error(f"Analysis failed: {e}")
        raise click.ClickException(str(e))
    finally:
        try:
            await app.cleanup()
        except:
            pass


@cli.command()
@click.option('--addresses-file', type=click.Path(exists=True),
              help='JSON file containing wallet addresses')
@click.option('--addresses', help='Comma-separated wallet addresses')
@click.option('--batch-size', default=50, type=int, help='Processing batch size')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv']),
              default='table', help='Output format')
@click.option('--save-results', type=click.Path(), help='Save results to file')
@click.pass_context
@handle_async
async def batch(
        ctx: click.Context,
        addresses_file: Optional[str],
        addresses: Optional[str],
        batch_size: int,
        output_format: str,
        save_results: Optional[str]
):
    """Process a batch of wallet addresses."""

    # Parse addresses
    wallet_addresses = []

    if addresses_file:
        try:
            with open(addresses_file, 'r') as f:
                data = json.load(f)

            if isinstance(data, list):
                for i, addr in enumerate(data):
                    if isinstance(addr, str):
                        wallet_addresses.append({
                            "address": addr,
                            "label": f"Wallet {i + 1}",
                            "row_number": i + 1
                        })
                    elif isinstance(addr, dict):
                        wallet_addresses.append({
                            "address": addr.get("address", ""),
                            "label": addr.get("label", f"Wallet {i + 1}"),
                            "row_number": i + 1
                        })

        except Exception as e:
            print_error(f"Failed to load addresses file: {e}")
            raise click.ClickException(str(e))

    elif addresses:
        addr_list = [addr.strip() for addr in addresses.split(',')]
        for i, addr in enumerate(addr_list):
            wallet_addresses.append({
                "address": addr,
                "label": f"Wallet {i + 1}",
                "row_number": i + 1
            })

    else:
        print_error("Either --addresses-file or --addresses must be provided")
        raise click.ClickException("No addresses provided")

    if not wallet_addresses:
        print_error("No valid wallet addresses found")
        raise click.ClickException("No valid addresses")

    try:
        # Initialize application
        app = get_app()
        await app.initialize()

        print_info(f"Processing {len(wallet_addresses)} wallet addresses")

        # Process wallets
        results = await app.process_wallet_list(
            addresses=wallet_addresses,
            dry_run=ctx.obj.get('dry_run', False)
        )

        # Display results
        await _display_results(results, output_format, save_results)

    except Exception as e:
        print_error(f"Batch processing failed: {e}")
        raise click.ClickException(str(e))
    finally:
        try:
            await app.cleanup()
        except:
            pass


@cli.command()
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.option('--save', type=click.Path(), help='Save health status to file')
@handle_async
async def health(output_format: str, save: Optional[str]):
    """Check health status of all services."""

    try:
        app = get_app()

        with console.status("[bold blue]Initializing application..."):
            await app.initialize()

        print_info("Checking service health...")

        health_status = await app.get_health_status()

        if output_format == 'json':
            output = json.dumps(health_status, indent=2)
            console.print(output)
        else:
            # Display as table
            health_table = Table(title="Service Health Status", box=box.ROUNDED)
            health_table.add_column("Service", style="cyan")
            health_table.add_column("Status", style="white")
            health_table.add_column("Details", style="dim")

            for service, is_healthy in health_status.items():
                status_emoji = "✅" if is_healthy else "❌"
                status_text = "Healthy" if is_healthy else "Unhealthy"
                status_style = "green" if is_healthy else "red"

                health_table.add_row(
                    service.replace('_', ' ').title(),
                    f"{status_emoji} [{status_style}]{status_text}[/{status_style}]",
                    "Operational" if is_healthy else "Check configuration"
                )

            console.print(health_table)

        # Save to file if requested
        if save:
            with open(save, 'w') as f:
                json.dump(health_status, f, indent=2)
            print_success(f"Health status saved to {save}")

        # Exit with error code if any service is unhealthy
        if not all(health_status.values()):
            raise click.ClickException("Some services are unhealthy")

    except Exception as e:
        print_error(f"Health check failed: {e}")
        raise click.ClickException(str(e))
    finally:
        try:
            await app.cleanup()
        except:
            pass


@cli.command()
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']),
              default='table', help='Output format')
@click.option('--save', type=click.Path(), help='Save metrics to file')
@click.option('--component', help='Show metrics for specific component only')
@handle_async
async def metrics(output_format: str, save: Optional[str], component: Optional[str]):
    """Show application metrics and statistics."""

    try:
        app = get_app()

        with console.status("[bold blue]Initializing application..."):
            await app.initialize()

        print_info("Collecting metrics...")

        all_metrics = await app.get_metrics()

        # Filter by component if specified
        if component:
            if component in all_metrics:
                metrics_data = {component: all_metrics[component]}
            else:
                available = ', '.join(all_metrics.keys())
                print_error(f"Component '{component}' not found. Available: {available}")
                raise click.ClickException("Invalid component")
        else:
            metrics_data = all_metrics

        if output_format == 'json':
            output = json.dumps(metrics_data, indent=2, cls=DecimalJSONEncoder)
            console.print(output)
        else:
            # Display as tables
            for comp_name, comp_metrics in metrics_data.items():
                if isinstance(comp_metrics, dict):
                    metrics_table = Table(
                        title=f"{comp_name.replace('_', ' ').title()} Metrics",
                        box=box.ROUNDED
                    )
                    metrics_table.add_column("Metric", style="cyan")
                    metrics_table.add_column("Value", style="white")

                    for key, value in comp_metrics.items():
                        # Format value nicely
                        if isinstance(value, (int, float)):
                            if key.endswith('_percent') or key.endswith('_rate'):
                                formatted_value = f"{value:.1f}%"
                            elif key.endswith('_seconds') or key.endswith('_time'):
                                formatted_value = f"{value:.2f}s"
                            elif key.endswith('_mb'):
                                formatted_value = f"{value:.1f} MB"
                            elif isinstance(value, int) and value > 1000:
                                formatted_value = f"{value:,}"
                            else:
                                formatted_value = str(value)
                        else:
                            formatted_value = str(value)

                        metrics_table.add_row(
                            key.replace('_', ' ').title(),
                            formatted_value
                        )

                    console.print(metrics_table)
                    console.print()

        # Save to file if requested
        if save:
            with open(save, 'w') as f:
                json.dump(metrics_data, f, indent=2, cls=DecimalJSONEncoder)
            print_success(f"Metrics saved to {save}")

    except Exception as e:
        print_error(f"Failed to collect metrics: {e}")
        raise click.ClickException(str(e))
    finally:
        try:
            await app.cleanup()
        except:
            pass


@cli.command()
@click.option('--check-config', is_flag=True, help='Validate configuration file')
@click.option('--check-credentials', is_flag=True, help='Test API credentials')
@click.option('--check-sheets', help='Test Google Sheets access with spreadsheet ID')
@handle_async
async def validate(check_config: bool, check_credentials: bool, check_sheets: Optional[str]):
    """Validate configuration and connectivity."""

    validation_errors = []

    try:
        # Check configuration
        if check_config:
            console.print("🔍 Validating configuration...")

            try:
                from .config import get_settings
                settings = get_settings()
                validation_result = settings.validate_config()

                if validation_result["valid"]:
                    print_success("Configuration is valid")

                    if validation_result["warnings"]:
                        for warning in validation_result["warnings"]:
                            print_warning(warning)
                else:
                    for issue in validation_result["issues"]:
                        print_error(f"Configuration issue: {issue}")
                        validation_errors.append(issue)

            except SettingsError as e:
                print_error(f"Configuration error: {e}")
                validation_errors.append(str(e))

        # Check API credentials
        if check_credentials:
            console.print("🔑 Testing API credentials...")

            app = get_app()
            await app.initialize()

            health_status = await app.get_health_status()

            for service, is_healthy in health_status.items():
                if is_healthy:
                    print_success(f"{service}: Credentials valid")
                else:
                    print_error(f"{service}: Credentials invalid or service unavailable")
                    validation_errors.append(f"{service} authentication failed")

        # Check Google Sheets access
        if check_sheets:
            console.print(f"📊 Testing Google Sheets access: {check_sheets}")

            app = get_app()
            await app.initialize()

            try:
                # Try to read from the specified sheet
                test_addresses = await app.sheets_client.read_wallet_addresses(
                    spreadsheet_id=check_sheets,
                    range_name="A1:B2",  # Small range for testing
                    skip_header=False
                )

                print_success(f"Successfully accessed spreadsheet {check_sheets}")
                print_info(f"Found {len(test_addresses)} rows in test range")

            except Exception as e:
                print_error(f"Failed to access spreadsheet: {e}")
                validation_errors.append(f"Google Sheets access failed: {e}")

        if validation_errors:
            console.print(f"\n❌ Validation completed with {len(validation_errors)} error(s)")
            raise click.ClickException("Validation failed")
        else:
            print_success("All validations passed!")

    except Exception as e:
        if not isinstance(e, click.ClickException):
            print_error(f"Validation failed: {e}")
            raise click.ClickException(str(e))
        raise
    finally:
        try:
            if 'app' in locals():
                await app.cleanup()
        except:
            pass


@cli.command()
@handle_async
async def interactive():
    """Start interactive mode."""

    try:
        app = get_app()
        await app.initialize()
        await app.run_interactive_mode()
    except Exception as e:
        print_error(f"Interactive mode failed: {e}")
        raise click.ClickException(str(e))
    finally:
        try:
            await app.cleanup()
        except:
            pass


async def _display_results(results: Dict[str, Any], output_format: str, save_path: Optional[str]) -> None:
    """Display processing results in the specified format."""

    if output_format == 'json':
        # JSON output
        output = json.dumps(results, indent=2, cls=DecimalJSONEncoder)
        console.print(output)

    elif output_format == 'csv':
        # CSV output (simplified)
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['Metric', 'Value'])

        # Write key metrics
        if 'results' in results:
            writer.writerow(['Wallets Processed', results['results'].get('processed', 0)])
            writer.writerow(['Wallets Skipped', results['results'].get('skipped', 0)])
            writer.writerow(['Wallets Failed', results['results'].get('failed', 0)])
            writer.writerow(['Success Rate %', results['results'].get('success_rate', 0)])

        if 'portfolio_values' in results:
            writer.writerow(['Total Value USD', results['portfolio_values'].get('total_usd', 0)])
            writer.writerow(['Average Value USD', results['portfolio_values'].get('average_usd', 0)])
            writer.writerow(['Median Value USD', results['portfolio_values'].get('median_usd', 0)])

        console.print(output.getvalue())

    else:
        # Table output (default)
        _display_results_table(results)

    # Save to file if requested
    if save_path:
        with open(save_path, 'w') as f:
            if save_path.endswith('.json'):
                json.dump(results, f, indent=2, cls=DecimalJSONEncoder)
            elif save_path.endswith('.csv'):
                # Save as CSV
                import csv
                writer = csv.writer(f)
                writer.writerow(['Metric', 'Value'])

                # Flatten results for CSV
                def flatten_dict(d, prefix=''):
                    for k, v in d.items():
                        if isinstance(v, dict):
                            yield from flatten_dict(v, f"{prefix}{k}.")
                        else:
                            yield f"{prefix}{k}", v

                for key, value in flatten_dict(results):
                    writer.writerow([key, value])
            else:
                # Default to JSON
                json.dump(results, f, indent=2, cls=DecimalJSONEncoder)

        print_success(f"Results saved to {save_path}")


def _display_results_table(results: Dict[str, Any]) -> None:
    """Display results in table format."""

    # Summary table
    summary_table = Table(title="📊 Processing Summary", box=box.ROUNDED)
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="white")

    # Input metrics
    if 'input' in results:
        summary_table.add_row("Total Wallets Input", str(results['input'].get('total_wallets', 0)))

    # Results metrics
    if 'results' in results:
        res = results['results']
        summary_table.add_row("✅ Processed", str(res.get('processed', 0)))
        summary_table.add_row("⏭️ Skipped", str(res.get('skipped', 0)))
        summary_table.add_row("❌ Failed", str(res.get('failed', 0)))
        summary_table.add_row("📈 Success Rate", f"{res.get('success_rate', 0):.1f}%")

    console.print(summary_table)
    console.print()

    # Portfolio values table
    if 'portfolio_values' in results:
        values_table = Table(title="💰 Portfolio Values", box=box.ROUNDED)
        values_table.add_column("Metric", style="cyan")
        values_table.add_column("Amount (USD)", style="green")

        pv = results['portfolio_values']
        values_table.add_row("Total Value", f"${pv.get('total_usd', 0):,.2f}")
        values_table.add_row("Average Value", f"${pv.get('average_usd', 0):,.2f}")
        values_table.add_row("Median Value", f"${pv.get('median_usd', 0):,.2f}")
        values_table.add_row("Maximum Value", f"${pv.get('max_usd', 0):,.2f}")
        values_table.add_row("Minimum Value", f"${pv.get('min_usd', 0):,.2f}")

        console.print(values_table)
        console.print()

    # Activity table
    if 'activity' in results:
        activity_table = Table(title="🎯 Wallet Activity", box=box.ROUNDED)
        activity_table.add_column("Metric", style="cyan")
        activity_table.add_column("Count", style="white")
        activity_table.add_column("Percentage", style="yellow")

        act = results['activity']
        total_analyzed = act.get('active_wallets', 0) + act.get('inactive_wallets', 0)

        if total_analyzed > 0:
            active_pct = (act.get('active_wallets', 0) / total_analyzed) * 100
            inactive_pct = (act.get('inactive_wallets', 0) / total_analyzed) * 100
        else:
            active_pct = inactive_pct = 0

        activity_table.add_row("✅ Active Wallets", str(act.get('active_wallets', 0)), f"{active_pct:.1f}%")
        activity_table.add_row("❌ Inactive Wallets", str(act.get('inactive_wallets', 0)), f"{inactive_pct:.1f}%")

        console.print(activity_table)
        console.print()

    # Token holders table
    if 'token_holders' in results:
        tokens_table = Table(title="🪙 Token Holdings", box=box.ROUNDED)
        tokens_table.add_column("Token", style="cyan")
        tokens_table.add_column("Holders", style="white")

        th = results['token_holders']
        tokens_table.add_row("ETH", str(th.get('eth', 0)))
        tokens_table.add_row("USDC", str(th.get('usdc', 0)))
        tokens_table.add_row("USDT", str(th.get('usdt', 0)))
        tokens_table.add_row("DAI", str(th.get('dai', 0)))

        console.print(tokens_table)
        console.print()

    # Performance table
    if 'performance' in results:
        perf_table = Table(title="⚡ Performance Metrics", box=box.ROUNDED)
        perf_table.add_column("Metric", style="cyan")
        perf_table.add_column("Value", style="white")

        perf = results['performance']
        perf_table.add_row("Total Time", f"{perf.get('total_time_seconds', 0):.1f}s")
        perf_table.add_row("Avg Time/Wallet", f"{perf.get('average_time_per_wallet', 0):.2f}s")
        perf_table.add_row("Cache Hit Rate", f"{perf.get('cache_hit_rate', 0):.1f}%")
        perf_table.add_row("API Calls", str(perf.get('api_calls_total', 0)))

        console.print(perf_table)


if __name__ == '__main__':
    cli()