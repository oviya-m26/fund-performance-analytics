import requests

def examine_amfi_format():
    """
    Examine the AMFI data format to understand the correct column structure.
    """
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        lines = response.text.splitlines()
        
        print("=== AMFI Data Format Analysis ===")
        print(f"Total lines: {len(lines)}")
        print()
        
        # Look at first few lines to understand structure
        for i, line in enumerate(lines[:10]):
            parts = [p.strip() for p in line.split(";")]
            print(f"Line {i+1}: {len(parts)} columns")
            print(f"Raw: {line[:200]}...")
            print(f"Parts: {parts}")
            print()
        
        # Look for a scheme line (should have scheme code > 3 digits)
        print("=== Looking for Scheme Lines ===")
        scheme_count = 0
        for i, line in enumerate(lines):
            if i > 100:  # Skip first 100 lines to avoid headers
                parts = [p.strip() for p in line.split(";")]
                if len(parts) >= 6 and parts[0].isdigit() and len(parts[0]) > 3:
                    print(f"Scheme line {scheme_count + 1}:")
                    print(f"  Raw: {line[:300]}...")
                    print(f"  Parts: {parts}")
                    print()
                    scheme_count += 1
                    if scheme_count >= 5:  # Show first 5 scheme lines
                        break
        
        # Look for AMC lines (should have AMC code <= 3 digits)
        print("=== Looking for AMC Lines ===")
        amc_count = 0
        for i, line in enumerate(lines):
            if i > 100:  # Skip first 100 lines to avoid headers
                parts = [p.strip() for p in line.split(";")]
                if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) <= 3:
                    print(f"AMC line {amc_count + 1}:")
                    print(f"  Raw: {line[:200]}...")
                    print(f"  Parts: {parts}")
                    print()
                    amc_count += 1
                    if amc_count >= 3:  # Show first 3 AMC lines
                        break
                        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    examine_amfi_format()

